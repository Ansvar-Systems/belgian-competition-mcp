/**
 * Ingestion crawler for the BMA (Belgische Mededingingsautoriteit / Autorité
 * belge de la Concurrence) MCP server.
 *
 * Scrapes competition decisions, merger control decisions, and sector data
 * from bma-abc.be (Dutch) and populates the SQLite database.
 *
 * Data sources:
 *   - Paginated decision listing (bma-abc.be/nl/beslissingen?page=N)
 *   - Individual decision detail pages (/nl/beslissingen/{slug})
 *   - Case types: CC (concentrations), RPR (restrictive practices),
 *     I/O (investigations), VM (interim measures), AR (court rulings)
 *
 * Content is published in the procedural language (French or Dutch).
 * The crawler fetches the Dutch listing but decision text may be in either
 * language depending on the proceeding.
 *
 * Usage:
 *   npx tsx scripts/ingest-bma.ts
 *   npx tsx scripts/ingest-bma.ts --dry-run
 *   npx tsx scripts/ingest-bma.ts --resume
 *   npx tsx scripts/ingest-bma.ts --force
 *   npx tsx scripts/ingest-bma.ts --max-pages 10
 */

import Database from "better-sqlite3";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { dirname, join } from "node:path";
import * as cheerio from "cheerio";
import { SCHEMA_SQL } from "../src/db.js";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DB_PATH = process.env["BCA_DB_PATH"] ?? "data/bca.db";
const STATE_FILE = join(dirname(DB_PATH), "ingest-state.json");
const BASE_URL = "https://www.bma-abc.be";
const LISTING_PATH = "/nl/beslissingen";
const MAX_LISTING_PAGES = 160; // ~155 pages as of 2026-03, with headroom
const RATE_LIMIT_MS = 1500;
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 5000;
const ITEMS_PER_PAGE = 10;
const USER_AGENT =
  "AnsvarBMACrawler/1.0 (+https://github.com/Ansvar-Systems/belgian-competition-mcp)";

// CLI flags
const dryRun = process.argv.includes("--dry-run");
const resume = process.argv.includes("--resume");
const force = process.argv.includes("--force");
const maxPagesArg = process.argv.find((_, i, a) => a[i - 1] === "--max-pages");
const maxPages = maxPagesArg ? parseInt(maxPagesArg, 10) : MAX_LISTING_PAGES;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface IngestState {
  processedUrls: string[];
  lastRun: string;
  decisionsIngested: number;
  mergersIngested: number;
  errors: string[];
}

interface ListingEntry {
  title: string;
  url: string;
  date: string | null;
  caseRef: string | null;
}

interface ParsedDecision {
  case_number: string;
  title: string;
  date: string | null;
  type: string | null;
  sector: string | null;
  parties: string | null;
  summary: string | null;
  full_text: string;
  outcome: string | null;
  fine_amount: number | null;
  gwb_articles: string | null;
  status: string;
}

interface ParsedMerger {
  case_number: string;
  title: string;
  date: string | null;
  sector: string | null;
  acquiring_party: string | null;
  target: string | null;
  summary: string | null;
  full_text: string;
  outcome: string | null;
  turnover: number | null;
}

interface SectorAccumulator {
  [id: string]: {
    name: string;
    name_en: string | null;
    description: string | null;
    decisionCount: number;
    mergerCount: number;
  };
}

// ---------------------------------------------------------------------------
// HTTP fetching with rate limiting and retries
// ---------------------------------------------------------------------------

let lastRequestTime = 0;

async function rateLimitedFetch(url: string): Promise<string | null> {
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < RATE_LIMIT_MS) {
    await sleep(RATE_LIMIT_MS - elapsed);
  }

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      lastRequestTime = Date.now();
      const response = await fetch(url, {
        headers: {
          "User-Agent": USER_AGENT,
          Accept:
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "nl-BE,nl;q=0.9,fr-BE;q=0.8,fr;q=0.7,en;q=0.5",
        },
        redirect: "follow",
        signal: AbortSignal.timeout(30_000),
      });

      if (response.status === 403 || response.status === 429) {
        console.warn(
          `  [WARN] HTTP ${response.status} for ${url} (attempt ${attempt}/${MAX_RETRIES})`,
        );
        if (attempt < MAX_RETRIES) {
          await sleep(RETRY_DELAY_MS * attempt);
          continue;
        }
        return null;
      }

      if (!response.ok) {
        console.warn(`  [WARN] HTTP ${response.status} for ${url}`);
        return null;
      }

      return await response.text();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.warn(
        `  [WARN] Fetch error for ${url} (attempt ${attempt}/${MAX_RETRIES}): ${message}`,
      );
      if (attempt < MAX_RETRIES) {
        await sleep(RETRY_DELAY_MS * attempt);
      }
    }
  }

  return null;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// State management (for --resume)
// ---------------------------------------------------------------------------

function loadState(): IngestState {
  if (resume && existsSync(STATE_FILE)) {
    try {
      const raw = readFileSync(STATE_FILE, "utf-8");
      return JSON.parse(raw) as IngestState;
    } catch {
      console.warn("[WARN] Could not read state file, starting fresh.");
    }
  }
  return {
    processedUrls: [],
    lastRun: new Date().toISOString(),
    decisionsIngested: 0,
    mergersIngested: 0,
    errors: [],
  };
}

function saveState(state: IngestState): void {
  state.lastRun = new Date().toISOString();
  const dir = dirname(STATE_FILE);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  writeFileSync(STATE_FILE, JSON.stringify(state, null, 2), "utf-8");
}

// ---------------------------------------------------------------------------
// Listing page parsing — discover decision URLs
// ---------------------------------------------------------------------------

/**
 * Parse a single listing page and extract decision entries.
 *
 * The BMA listing at /nl/beslissingen?page=N renders ~8-10 decisions per page.
 * Each entry has a linked title (pointing to /nl/beslissingen/{slug}), a date,
 * and sometimes PDF download links.
 */
function parseListingPage(html: string): ListingEntry[] {
  const $ = cheerio.load(html);
  const entries: ListingEntry[] = [];

  // Decision entries are rendered as linked items within the main content.
  // Each entry contains a link to the detail page with the case title.
  // We look for links whose href matches the beslissingen path pattern.
  const seen = new Set<string>();

  $("a[href]").each((_i, el) => {
    const href = $(el).attr("href") ?? "";
    const text = $(el).text().trim();

    // Match decision detail links: /nl/beslissingen/{slug}
    // Exclude the listing page itself and filter/pagination links
    if (
      !href.match(/^\/nl\/beslissingen\/[\w-]/) ||
      href === LISTING_PATH ||
      href.includes("?")
    ) {
      return;
    }

    // Skip PDF download links
    if (href.endsWith(".pdf")) return;

    // Skip duplicate URLs on the same page
    if (seen.has(href)) return;
    seen.add(href);

    // Skip empty link text or navigation-only links
    if (!text || text.length < 3) return;

    // Extract the case reference from the title text or URL slug.
    // Title format: "24-CC-46-AUD: SAAEM Belgium SA / TB NV" or just
    // "SAAEM Belgium SA / TB NV" with the ref in the URL slug.
    const caseRef = extractCaseRefFromSlug(href) ?? extractCaseRefFromText(text);

    // Try to find a date near this link. BMA pages show dates as text
    // near the decision title (e.g. "09 december 2024").
    const parent = $(el).closest("div, li, article, tr, .view-content > div");
    const parentText = parent.length > 0 ? parent.text() : "";
    const date = extractDateFromText(parentText);

    entries.push({
      title: text,
      url: href.startsWith("http") ? href : `${BASE_URL}${href}`,
      date,
      caseRef,
    });
  });

  return entries;
}

/**
 * Extract a case reference from a URL slug.
 *
 * Slug patterns:
 *   /nl/beslissingen/24-cc-46-aud-saaem-belgium-sa-tb-nv-et-nb-nv
 *   /nl/beslissingen/25-rpr-40-base-bmb
 *   /nl/beslissingen/2024ar750-koninklijke-belgische-biljartbond-vzw
 *   /nl/beslissingen/94-cc-21-sulzer-perkin-elmer
 */
function extractCaseRefFromSlug(href: string): string | null {
  const slug = href.split("/nl/beslissingen/").pop() ?? "";

  // Modern format: YY-CC-NN[-AUD], YY-RPR-NN[-AUD], YY-IO-NN, YY-VM-NN
  const modernMatch = slug.match(
    /^(\d{2}-(?:cc|rpr|io|vm|ccs|pk|ab)-\d{1,3}(?:-aud)?)/i,
  );
  if (modernMatch) {
    return modernMatch[1]!.toUpperCase();
  }

  // Court ruling format: YYYYARNNNN
  const courtMatch = slug.match(/^(\d{4}ar\d+)/i);
  if (courtMatch) {
    return courtMatch[1]!.toUpperCase();
  }

  // Older I/O format with different separators
  const oldMatch = slug.match(
    /^(\d{2,4}[-_]?(?:io|vm|cc|rpr)[-_]?\d{1,3})/i,
  );
  if (oldMatch) {
    return oldMatch[1]!.toUpperCase().replace(/_/g, "-");
  }

  return null;
}

/** Extract a case reference from the visible link text. */
function extractCaseRefFromText(text: string): string | null {
  // "24-CC-46-AUD: Some title" or "BMA-2024-CC-46-AUD"
  const bmaMatch = text.match(
    /(?:BMA|ABC|MEDE)[-\s]*(\d{2,4}[-\s]*(?:CC|RPR|IO|VM|CCS|PK|AB)[-\s]*\d{1,3}(?:[-\s]*AUD)?)/i,
  );
  if (bmaMatch) {
    return bmaMatch[1]!.replace(/\s+/g, "-").toUpperCase();
  }

  // Plain case ref at start of text
  const plainMatch = text.match(
    /^(\d{2}-(?:CC|RPR|IO|VM|CCS|PK|AB)-\d{1,3}(?:-AUD)?)/i,
  );
  if (plainMatch) {
    return plainMatch[1]!.toUpperCase();
  }

  return null;
}

// ---------------------------------------------------------------------------
// Date parsing — handles Dutch and French month names
// ---------------------------------------------------------------------------

const MONTH_MAP: Record<string, string> = {
  // Dutch
  januari: "01",
  februari: "02",
  maart: "03",
  april: "04",
  mei: "05",
  juni: "06",
  juli: "07",
  augustus: "08",
  september: "09",
  oktober: "10",
  november: "11",
  december: "12",
  // French
  janvier: "01",
  février: "02",
  mars: "03",
  // avril already matches Dutch april
  // mai already matches Dutch mei close enough — handled separately
  juin: "06",
  juillet: "07",
  août: "08",
  // septembre already matches
  octobre: "10",
  // novembre already matches
  décembre: "12",
};

/**
 * Parse a Dutch or French date string to ISO format.
 *
 * Handles: "09 december 2024", "26 januari 2026", "12 Février 2026",
 * "dd/mm/yyyy", "dd-mm-yyyy", "yyyy-mm-dd".
 */
function parseDate(raw: string): string | null {
  if (!raw) return null;

  // ISO format already
  const isoMatch = raw.match(/(\d{4})-(\d{2})-(\d{2})/);
  if (isoMatch) return isoMatch[0];

  // dd/mm/yyyy or dd-mm-yyyy
  const numericMatch = raw.match(/(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{4})/);
  if (numericMatch) {
    const [, day, month, year] = numericMatch;
    return `${year}-${month!.padStart(2, "0")}-${day!.padStart(2, "0")}`;
  }

  // "dd month yyyy" — Dutch or French
  const textMatch = raw.match(/(\d{1,2})\s+([a-zA-Zéûô]+)\s+(\d{4})/);
  if (textMatch) {
    const [, day, monthName, year] = textMatch;
    const monthNum = MONTH_MAP[monthName!.toLowerCase()];
    if (monthNum) {
      return `${year}-${monthNum}-${day!.padStart(2, "0")}`;
    }
  }

  return null;
}

/** Try to find a date in a block of text. */
function extractDateFromText(text: string): string | null {
  // Look for "dd month yyyy" pattern
  const datePattern =
    /(\d{1,2})\s+(januari|februari|maart|april|mei|juni|juli|augustus|september|oktober|november|december|janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre)\s+(\d{4})/i;
  const match = text.match(datePattern);
  if (match) {
    return parseDate(match[0]);
  }

  // dd/mm/yyyy
  const numMatch = text.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (numMatch) {
    return parseDate(numMatch[0]);
  }

  return null;
}

// ---------------------------------------------------------------------------
// Detail page parsing — extract structured data
// ---------------------------------------------------------------------------

/**
 * Extract metadata fields from a BMA decision detail page.
 *
 * BMA detail pages contain labelled metadata:
 *   - Dossier nr / Numéro de dossier
 *   - Beslissing nr / Numéro de décision
 *   - Sector / Secteur
 *   - Type dossier
 *   - Klager / Plaignant (complainant)
 *   - Verweerder / Partie adverse (respondent)
 *   - Koper / Acquéreur (buyer in merger)
 *   - Verkoper / Cédant (seller in merger)
 */
function extractMetadata(
  $: cheerio.CheerioAPI,
): Record<string, string> {
  const meta: Record<string, string> = {};

  // Pattern 1: Definition list (dl/dt/dd)
  $("dl dt").each((_i, el) => {
    const label = $(el).text().trim().replace(/:$/, "").toLowerCase();
    const dd = $(el).next("dd");
    if (dd.length > 0) {
      const value = dd.text().trim();
      if (value) meta[label] = value;
    }
  });

  // Pattern 2: Drupal field wrappers (.field--label + .field--item)
  $(".field, .field--item").each((_i, el) => {
    const labelEl = $(el).find(
      ".field--label, .field-label, .field__label",
    );
    const valueEl = $(el).find(
      ".field--item, .field-item, .field__item",
    );
    if (labelEl.length > 0 && valueEl.length > 0) {
      const label = labelEl.text().trim().replace(/:$/, "").toLowerCase();
      const value = valueEl.text().trim();
      if (label && value) meta[label] = value;
    }
  });

  // Pattern 3: Table rows (some older pages use tables for metadata)
  $("table tr").each((_i, el) => {
    const cells = $(el).find("td, th");
    if (cells.length >= 2) {
      const label = $(cells[0]).text().trim().replace(/:$/, "").toLowerCase();
      const value = $(cells[1]).text().trim();
      if (label && value) meta[label] = value;
    }
  });

  // Pattern 4: Strong/bold labels followed by text in paragraphs
  $("p, div").each((_i, el) => {
    const strong = $(el).find("strong, b").first();
    if (strong.length > 0) {
      const label = strong.text().trim().replace(/:$/, "").toLowerCase();
      // Get remaining text after the label
      const fullText = $(el).text().trim();
      const value = fullText.replace(strong.text().trim(), "").replace(/^[:\s]+/, "").trim();
      if (
        label &&
        value &&
        label.length < 60 &&
        (label.includes("dossier") ||
          label.includes("beslissing") ||
          label.includes("décision") ||
          label.includes("sector") ||
          label.includes("secteur") ||
          label.includes("koper") ||
          label.includes("verkoper") ||
          label.includes("acquéreur") ||
          label.includes("cédant") ||
          label.includes("klager") ||
          label.includes("verweerder") ||
          label.includes("plaignant") ||
          label.includes("partie") ||
          label.includes("datum") ||
          label.includes("date") ||
          label.includes("boete") ||
          label.includes("amende") ||
          label.includes("type"))
      ) {
        meta[label] = value;
      }
    }
  });

  // Pattern 5: Look for case numbers in page text
  const pageText = $("main, article, .content, .node").text();
  if (!meta["dossier nr"] && !meta["numéro de dossier"]) {
    const dossierMatch = pageText.match(
      /(?:MEDE|CONC)[-\s](?:CCS|CC|PK|IO|VM|RPR)[-\s]\d{2,4}[-\s]\d{3,5}/,
    );
    if (dossierMatch) {
      meta["dossier nr"] = dossierMatch[0];
    }
  }

  if (!meta["beslissing nr"] && !meta["numéro de décision"]) {
    const decisionMatch = pageText.match(
      /(?:BMA|ABC)[-\s]\d{4}[-\s](?:CCS|CC|RPR|IO|VM|PK|AB)[-\s]\d{1,3}(?:[-\s]AUD)?/i,
    );
    if (decisionMatch) {
      meta["beslissing nr"] = decisionMatch[0];
    }
  }

  return meta;
}

/**
 * Extract the main body text from a detail page.
 * BMA pages are sparse — most content is in PDFs. We extract whatever
 * text is available on the HTML page itself.
 */
function extractBodyText($: cheerio.CheerioAPI): string {
  const selectors = [
    "article .field--name-body",
    "article .body",
    ".node__content .field--name-body",
    ".node__content",
    ".content-area",
    "article .text-long",
    "main article",
    ".node--type-decision .content",
  ];

  for (const sel of selectors) {
    const el = $(sel);
    if (el.length > 0) {
      // Remove navigation, breadcrumb, and script elements
      el.find("nav, .breadcrumb, script, style, .pager").remove();
      const text = el.text().trim();
      if (text.length > 50) return text;
    }
  }

  // Fallback: collect paragraph text from main area
  const paragraphs: string[] = [];
  $("main p, article p, .content p").each((_i, el) => {
    const text = $(el).text().trim();
    if (text.length > 20) paragraphs.push(text);
  });
  if (paragraphs.length > 0) return paragraphs.join("\n\n");

  // Last resort: strip nav/footer and grab main text
  $("nav, footer, header, .menu, .breadcrumb, script, style").remove();
  return $("main, article, .content").text().trim();
}

/**
 * Extract PDF download URLs from the detail page.
 */
function extractPdfLinks($: cheerio.CheerioAPI): string[] {
  const pdfs: string[] = [];
  $('a[href$=".pdf"]').each((_i, el) => {
    const href = $(el).attr("href") ?? "";
    if (href) {
      const fullUrl = href.startsWith("http") ? href : `${BASE_URL}${href}`;
      pdfs.push(fullUrl);
    }
  });
  return [...new Set(pdfs)];
}

// ---------------------------------------------------------------------------
// Classification — case type, outcome, sector
// ---------------------------------------------------------------------------

/**
 * Determine whether a case is a merger (CC) or enforcement decision (RPR/IO/etc.).
 * Also classify the decision type and outcome.
 */
function classifyCase(
  caseRef: string | null,
  meta: Record<string, string>,
  title: string,
  bodyText: string,
): {
  isMerger: boolean;
  isDecision: boolean;
  type: string | null;
  outcome: string | null;
} {
  const caseUpper = (caseRef ?? "").toUpperCase();
  const titleLower = title.toLowerCase();
  const metaType = (
    meta["type dossier"] ??
    meta["type de dossier"] ??
    ""
  ).toLowerCase();
  const allText = `${titleLower} ${metaType} ${bodyText.toLowerCase().slice(0, 3000)}`;

  // --- Merger detection ---
  // CC and CCS case codes indicate concentrations
  const isMerger =
    /\d{2}-CC[S]?-\d/.test(caseUpper) ||
    metaType.includes("concentratie") ||
    metaType.includes("concentration") ||
    allText.includes("inzake concentraties") ||
    allText.includes("en matière de concentrations") ||
    allText.includes("fusie") ||
    allText.includes("overname") && allText.includes("concentratie");

  // --- Decision type ---
  let type: string | null = null;

  if (
    metaType.includes("kartel") ||
    allText.includes("kartel") ||
    allText.includes("cartel") ||
    allText.includes("art. iv.1") ||
    allText.includes("article iv.1")
  ) {
    type = "cartel";
  } else if (
    metaType.includes("misbruik") ||
    metaType.includes("abus") ||
    allText.includes("misbruik van machtspositie") ||
    allText.includes("abus de position dominante") ||
    allText.includes("art. iv.2") ||
    allText.includes("article iv.2")
  ) {
    type = "abuse_of_dominance";
  } else if (
    allText.includes("sectoronderzoek") ||
    allText.includes("enquête sectorielle") ||
    allText.includes("marktonderzoek")
  ) {
    type = "sector_inquiry";
  } else if (
    /\d{2}-VM-\d/.test(caseUpper) ||
    allText.includes("voorlopige maatregel") ||
    allText.includes("mesures provisoires")
  ) {
    type = "interim_measures";
  } else if (
    allText.includes("schikking") ||
    allText.includes("transaction")
  ) {
    type = "settlement";
  } else if (
    metaType.includes("boek v") ||
    allText.includes("boek v") ||
    allText.includes("livre v") ||
    allText.includes("economische afhankelijkheid") ||
    allText.includes("dépendance économique")
  ) {
    type = "abuse_of_economic_dependence";
  } else if (isMerger) {
    type = "merger_control";
  } else if (/\d{2}-RPR-\d/.test(caseUpper)) {
    type = "restrictive_practice";
  } else if (/\d{4}AR\d/.test(caseUpper)) {
    type = "court_ruling";
  }

  // --- Outcome ---
  let outcome: string | null = null;

  if (
    allText.includes("boete") ||
    allText.includes("amende") ||
    allText.includes("geldboete") ||
    allText.includes("beboet")
  ) {
    outcome = "fine";
  } else if (
    allText.includes("goedgekeurd") ||
    allText.includes("autorisée") ||
    allText.includes("autorisé") ||
    allText.includes("approuvée") ||
    (allText.includes("geen bezwaar") || allText.includes("pas d'objection"))
  ) {
    if (isMerger) {
      outcome =
        allText.includes("fase 2") ||
        allText.includes("fase ii") ||
        allText.includes("phase 2") ||
        allText.includes("phase ii")
          ? "cleared_phase2"
          : "cleared_phase1";
    } else {
      outcome = "cleared";
    }
  } else if (
    allText.includes("voorwaarden") ||
    allText.includes("conditions") ||
    allText.includes("verbintenissen") ||
    allText.includes("engagements")
  ) {
    outcome = isMerger ? "cleared_with_conditions" : "commitments";
  } else if (
    allText.includes("geweigerd") ||
    allText.includes("verboden") ||
    allText.includes("refusée") ||
    allText.includes("interdit")
  ) {
    outcome = "blocked";
  } else if (
    allText.includes("schikking") ||
    allText.includes("transaction") ||
    allText.includes("settlement")
  ) {
    outcome = "settlement";
  } else if (
    allText.includes("seponering") ||
    allText.includes("classement") ||
    allText.includes("afgesloten") ||
    allText.includes("clôtur")
  ) {
    outcome = "closed";
  } else if (isMerger) {
    // Most BMA merger decisions with no clear indicator are approvals
    outcome = "cleared_phase1";
  }

  const isDecision = !isMerger && type !== null;

  return { isMerger, isDecision, type, outcome };
}

/**
 * Map metadata and text content to a sector identifier.
 * Uses both Dutch and French terms since BMA content appears in either language.
 */
function classifySector(
  meta: Record<string, string>,
  title: string,
  bodyText: string,
): string | null {
  const sectorField = (
    meta["sector"] ??
    meta["secteur"] ??
    meta["economische sector"] ??
    meta["secteur économique"] ??
    ""
  ).toLowerCase();

  const text =
    `${sectorField} ${title} ${bodyText.slice(0, 2000)}`.toLowerCase();

  const mapping: Array<{ id: string; patterns: string[] }> = [
    {
      id: "energie",
      patterns: [
        "energie",
        "énergie",
        "elektriciteit",
        "électricité",
        "gas",
        "gaz",
        "elia",
        "fluxys",
        "engie",
        "luminus",
      ],
    },
    {
      id: "distribution_alimentaire",
      patterns: [
        "alimentaire",
        "voedseldistributie",
        "levensmiddel",
        "supermarkt",
        "supermarché",
        "colruyt",
        "delhaize",
        "carrefour",
        "distribution alimentaire",
        "detailhandel voeding",
      ],
    },
    {
      id: "telecommunications",
      patterns: [
        "telecom",
        "télécommunication",
        "breedband",
        "haut débit",
        "proximus",
        "telenet",
        "orange belgium",
        "mobiel",
        "mobile",
        "bipt",
        "ibpt",
      ],
    },
    {
      id: "sante",
      patterns: [
        "santé",
        "gezondheidszorg",
        "ziekenhuis",
        "hôpital",
        "farmac",
        "pharmaceut",
        "geneesmiddel",
        "médicament",
        "inami",
        "riziv",
      ],
    },
    {
      id: "banques",
      patterns: [
        "banque",
        "bank",
        "financ",
        "verzekering",
        "assurance",
        "betaalverkeer",
        "paiement",
        "belfius",
        "kbc",
        "bnp paribas fortis",
        "ing belg",
      ],
    },
    {
      id: "construction",
      patterns: [
        "bouw",
        "construction",
        "vastgoed",
        "immobilier",
        "aanbesteding",
        "marché public",
        "travaux",
      ],
    },
    {
      id: "transport",
      patterns: [
        "transport",
        "vervoer",
        "logistiek",
        "logistique",
        "luchtvaart",
        "aviation",
        "spoor",
        "chemin de fer",
        "sncb",
        "nmbs",
        "haven",
        "port",
      ],
    },
    {
      id: "digital",
      patterns: [
        "digitaal",
        "numérique",
        "digital",
        "platform",
        "plateforme",
        "e-commerce",
        "online",
        "internet",
      ],
    },
    {
      id: "media",
      patterns: [
        "media",
        "médias",
        "presse",
        "pers",
        "omroep",
        "radiodiffusion",
        "uitgever",
        "éditeur",
      ],
    },
    {
      id: "professions_liberales",
      patterns: [
        "beroepsvereniging",
        "ordre professionnel",
        "association professionnelle",
        "vrij beroep",
        "profession libérale",
        "avocat",
        "advocaat",
        "notaris",
        "notaire",
        "architect",
      ],
    },
    {
      id: "automobile",
      patterns: [
        "auto",
        "voiture",
        "véhicule",
        "voertuig",
        "concessionnaire",
        "dealer",
        "carrossier",
        "garage",
      ],
    },
    {
      id: "chimie",
      patterns: [
        "chimie",
        "chemie",
        "chimique",
        "chemisch",
        "solvay",
        "basf",
      ],
    },
    {
      id: "retail",
      patterns: [
        "retail",
        "détail",
        "kleinhandel",
        "commerce de détail",
        "winkelketen",
      ],
    },
    {
      id: "agriculture",
      patterns: [
        "agriculture",
        "landbouw",
        "agrar",
        "agricole",
        "veeteelt",
        "élevage",
      ],
    },
  ];

  for (const { id, patterns } of mapping) {
    for (const p of patterns) {
      if (text.includes(p)) return id;
    }
  }

  return null;
}

// ---------------------------------------------------------------------------
// Fine and article extraction
// ---------------------------------------------------------------------------

/**
 * Extract a fine amount from text. Handles Belgian/French number formatting
 * (dots as thousands separators, comma for decimal) and word-form amounts.
 */
function extractFineAmount(text: string): number | null {
  const patterns = [
    // "EUR 1.234.567" / "€ 1.234.567" / "1.234.567 EUR"
    /(?:€|EUR)\s*([\d.]+(?:,\d+)?)/gi,
    /([\d.]+(?:,\d+)?)\s*(?:€|EUR)/gi,
    // "N millions d'euros" / "N miljoen euro"
    /([\d.,]+)\s*million[s]?\s*(?:d'euros|euro)/gi,
    /([\d.,]+)\s*miljoen\s*euro/gi,
    // "amende de EUR N" / "boete van EUR N"
    /(?:amende|boete|geldboete)\s*(?:de|van)\s*(?:€|EUR)\s*([\d.]+(?:,\d+)?)/gi,
  ];

  for (const pattern of patterns) {
    const match = pattern.exec(text);
    if (match?.[1]) {
      let numStr = match[1];

      if (
        pattern.source.includes("million") ||
        pattern.source.includes("miljoen")
      ) {
        numStr = numStr.replace(/\./g, "").replace(",", ".");
        const val = parseFloat(numStr);
        if (!isNaN(val) && val > 0) return val * 1_000_000;
      }

      // Standard amount: dots are thousands separators, comma is decimal
      numStr = numStr.replace(/\./g, "").replace(",", ".");
      const val = parseFloat(numStr);
      if (!isNaN(val) && val > 0) return val;
    }
  }

  return null;
}

/**
 * Extract cited WER (Wetboek van economisch recht) / CDE (Code de droit
 * économique) articles and EU Treaty articles from text.
 */
function extractLegalArticles(text: string): string[] {
  const articles: Set<string> = new Set();

  // Art. IV.1 / Art. IV.2 WER/CDE
  const werPattern =
    /Art(?:ikel|icle)?\.?\s*(IV\.\d+(?:\s*§\s*\d+)?)\s*(?:WER|CDE|VWEU)?/gi;
  let m: RegExpExecArray | null;
  while ((m = werPattern.exec(text)) !== null) {
    articles.add(`Art. ${m[1]} WER`);
  }

  // Art. 101/102 VWEU/TFUE
  const euPattern =
    /Art(?:ikel|icle)?\.?\s*(101|102)\s*(?:VWEU|TFUE|TFEU|VwEU)/gi;
  while ((m = euPattern.exec(text)) !== null) {
    articles.add(`Art. ${m[1]} VWEU`);
  }

  // Art. IV.6 (merger notification) / Art. IV.9 (merger decision)
  const mergerPattern = /Art(?:ikel|icle)?\.?\s*(IV\.\d+)\s*(?:WER|CDE)/gi;
  while ((m = mergerPattern.exec(text)) !== null) {
    articles.add(`Art. ${m[1]} WER`);
  }

  return [...articles];
}

/** Extract acquiring and target parties from a merger title. */
function extractMergerParties(
  title: string,
  bodyText: string,
  meta: Record<string, string>,
): { acquiring: string | null; target: string | null } {
  // Check metadata first — BMA detail pages often have explicit buyer/seller
  const buyer =
    meta["koper"] ??
    meta["acquéreur"] ??
    meta["kopende partij"] ??
    meta["partie acquéreuse"] ??
    null;
  const seller =
    meta["verkoper"] ??
    meta["cédant"] ??
    meta["doelvennootschap"] ??
    meta["entreprise cible"] ??
    null;

  if (buyer || seller) {
    return { acquiring: buyer, target: seller };
  }

  // Title pattern: "Acquirer / Target" or "Acquirer – Target"
  const slashMatch = title.match(/^(.+?)\s*[/–—-]\s*(.+)$/);
  if (slashMatch) {
    // Strip case number prefix if present
    let acquirer = slashMatch[1]!.trim();
    const target = slashMatch[2]!.trim();
    // Remove leading case ref like "24-CC-46-AUD: "
    acquirer = acquirer.replace(
      /^\d{2}-\w+-\d+(?:-AUD)?[:\s]+/i,
      "",
    );
    return {
      acquiring: acquirer || null,
      target: target || null,
    };
  }

  return { acquiring: null, target: null };
}

/**
 * Build a case number for the database from available information.
 * Prefers the official BMA/ABC decision number, falls back to the
 * URL slug-derived reference.
 */
function buildCaseNumber(
  caseRef: string | null,
  meta: Record<string, string>,
  url: string,
): string {
  // Official decision number from metadata
  const decisionNr =
    meta["beslissing nr"] ??
    meta["numéro de décision"] ??
    meta["decision nr"] ??
    null;

  if (decisionNr) {
    // Normalise: "BMA-2024-CC-46-AUD" → "ABC-2024-CC-46-AUD"
    // The authority uses both ABC (French) and BMA (Dutch) prefixes.
    // We store with ABC- prefix for consistency with the seed data.
    return decisionNr
      .replace(/^BMA-/i, "ABC-")
      .replace(/\s+/g, "-")
      .toUpperCase();
  }

  // Construct from the case reference extracted from the listing
  if (caseRef) {
    // Expand 2-digit year to 4-digit for the ABC- prefix format
    const yearMatch = caseRef.match(/^(\d{2})-/);
    if (yearMatch) {
      const twoDigitYear = parseInt(yearMatch[1]!, 10);
      const fullYear = twoDigitYear >= 90 ? 1900 + twoDigitYear : 2000 + twoDigitYear;
      const rest = caseRef.slice(3);
      return `ABC-${fullYear}-${rest}`;
    }
    return `ABC-${caseRef}`;
  }

  // Last resort: derive from URL slug
  const slug = url.split("/nl/beslissingen/").pop() ?? "";
  const shortSlug = slug.slice(0, 60).replace(/-+$/, "");
  return `BMA-WEB/${shortSlug}`;
}

// ---------------------------------------------------------------------------
// Page parsing — combine everything into a decision or merger record
// ---------------------------------------------------------------------------

function parseDetailPage(
  html: string,
  url: string,
  listingEntry: ListingEntry,
): { decision: ParsedDecision | null; merger: ParsedMerger | null } {
  const $ = cheerio.load(html);

  // Title: prefer h1, fall back to og:title, then listing entry title
  const title =
    $("h1").first().text().trim() ||
    $('meta[property="og:title"]').attr("content")?.trim() ||
    $("title")
      .text()
      .trim()
      .replace(/ \| (?:Belgische Mededingingsautoriteit|Belgian Competition Authority)$/, "") ||
    listingEntry.title;

  if (!title) {
    return { decision: null, merger: null };
  }

  const meta = extractMetadata($);
  const bodyText = extractBodyText($);
  const pdfLinks = extractPdfLinks($);

  // Build full_text: combine all available text
  const fullTextParts: string[] = [];
  if (title) fullTextParts.push(title);

  // Add metadata as structured text
  const metaEntries = Object.entries(meta)
    .filter(([, v]) => v.length > 0)
    .map(([k, v]) => `${k}: ${v}`);
  if (metaEntries.length > 0) {
    fullTextParts.push(metaEntries.join("; "));
  }

  if (bodyText && bodyText.length > 50) {
    fullTextParts.push(bodyText);
  }

  if (pdfLinks.length > 0) {
    fullTextParts.push(`PDF documents: ${pdfLinks.join(", ")}`);
  }

  const fullText = fullTextParts.join("\n\n");

  // Minimum content threshold
  if (fullText.length < 30) {
    return { decision: null, merger: null };
  }

  // Date: prefer metadata, fall back to listing entry
  const rawDate =
    meta["datum"] ??
    meta["date"] ??
    meta["beslissingsdatum"] ??
    meta["date de décision"] ??
    "";
  const date = parseDate(rawDate) ?? listingEntry.date;

  // Case number
  const caseNumber = buildCaseNumber(
    listingEntry.caseRef,
    meta,
    url,
  );

  // Classification
  const { isMerger, isDecision, type, outcome } = classifyCase(
    listingEntry.caseRef,
    meta,
    title,
    fullText,
  );
  const sector = classifySector(meta, title, fullText);

  // Summary: first 500 chars of body text
  const summary =
    bodyText.length > 50
      ? bodyText.slice(0, 500).replace(/\s+/g, " ").trim()
      : null;

  if (isMerger) {
    const { acquiring, target } = extractMergerParties(
      title,
      bodyText,
      meta,
    );

    return {
      decision: null,
      merger: {
        case_number: caseNumber,
        title,
        date,
        sector,
        acquiring_party: acquiring,
        target: target,
        summary,
        full_text: fullText,
        outcome: outcome ?? "cleared_phase1",
        turnover: null, // Not reliably extractable from HTML
      },
    };
  }

  if (isDecision || type !== null) {
    const parties =
      meta["verweerder"] ??
      meta["partie adverse"] ??
      meta["klager"] ??
      meta["plaignant"] ??
      meta["partijen"] ??
      meta["parties"] ??
      null;

    const fineAmount = extractFineAmount(fullText);
    const articles = extractLegalArticles(fullText);

    return {
      decision: {
        case_number: caseNumber,
        title,
        date,
        type,
        sector,
        parties,
        summary,
        full_text: fullText,
        outcome: outcome ?? (fineAmount ? "fine" : "pending"),
        fine_amount: fineAmount,
        gwb_articles:
          articles.length > 0 ? articles.join(", ") : null,
        status: "final",
      },
      merger: null,
    };
  }

  // Cannot clearly classify — treat as a generic decision
  const fineAmount = extractFineAmount(fullText);
  const articles = extractLegalArticles(fullText);

  return {
    decision: {
      case_number: caseNumber,
      title,
      date,
      type: type ?? "decision",
      sector,
      parties: null,
      summary,
      full_text: fullText,
      outcome: outcome ?? (fineAmount ? "fine" : "pending"),
      fine_amount: fineAmount,
      gwb_articles:
        articles.length > 0 ? articles.join(", ") : null,
      status: "final",
    },
    merger: null,
  };
}

// ---------------------------------------------------------------------------
// Database operations
// ---------------------------------------------------------------------------

function initDb(): Database.Database {
  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
    console.log(`Created data directory: ${dir}`);
  }

  if (force && existsSync(DB_PATH)) {
    unlinkSync(DB_PATH);
    console.log(`Deleted existing database (--force)`);
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA_SQL);

  return db;
}

function prepareStatements(db: Database.Database) {
  const insertDecision = db.prepare(`
    INSERT OR IGNORE INTO decisions
      (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, gwb_articles, status)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const upsertDecision = db.prepare(`
    INSERT INTO decisions
      (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, gwb_articles, status)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(case_number) DO UPDATE SET
      title = excluded.title,
      date = excluded.date,
      type = excluded.type,
      sector = excluded.sector,
      parties = excluded.parties,
      summary = excluded.summary,
      full_text = excluded.full_text,
      outcome = excluded.outcome,
      fine_amount = excluded.fine_amount,
      gwb_articles = excluded.gwb_articles,
      status = excluded.status
  `);

  const insertMerger = db.prepare(`
    INSERT OR IGNORE INTO mergers
      (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const upsertMerger = db.prepare(`
    INSERT INTO mergers
      (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(case_number) DO UPDATE SET
      title = excluded.title,
      date = excluded.date,
      sector = excluded.sector,
      acquiring_party = excluded.acquiring_party,
      target = excluded.target,
      summary = excluded.summary,
      full_text = excluded.full_text,
      outcome = excluded.outcome,
      turnover = excluded.turnover
  `);

  const upsertSector = db.prepare(`
    INSERT INTO sectors (id, name, name_en, description, decision_count, merger_count)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      decision_count = excluded.decision_count,
      merger_count = excluded.merger_count
  `);

  return {
    insertDecision,
    upsertDecision,
    insertMerger,
    upsertMerger,
    upsertSector,
  };
}

// ---------------------------------------------------------------------------
// Main ingestion pipeline
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log("=== BMA Competition Decisions Crawler ===");
  console.log(`  Database:   ${DB_PATH}`);
  console.log(`  Dry run:    ${dryRun}`);
  console.log(`  Resume:     ${resume}`);
  console.log(`  Force:      ${force}`);
  console.log(`  Max pages:  ${maxPages}`);
  console.log("");

  // Load resume state
  const state = loadState();
  const processedSet = new Set(state.processedUrls);

  // ---- Step 1: Discover decision URLs from paginated listing ----
  console.log("Step 1: Discovering decision URLs from listing pages...\n");
  const allEntries: ListingEntry[] = [];
  let emptyPageStreak = 0;

  for (let page = 0; page < maxPages; page++) {
    const listingUrl = `${BASE_URL}${LISTING_PATH}?page=${page}`;
    console.log(`  Fetching listing page ${page + 1}/${maxPages}...`);

    const html = await rateLimitedFetch(listingUrl);
    if (!html) {
      console.warn(`  [WARN] Could not fetch listing page ${page}`);
      emptyPageStreak++;
      if (emptyPageStreak >= 3) {
        console.log("  Three consecutive empty pages — stopping discovery.");
        break;
      }
      continue;
    }

    const entries = parseListingPage(html);
    if (entries.length === 0) {
      emptyPageStreak++;
      console.log(`  No entries found on page ${page + 1}`);
      if (emptyPageStreak >= 3) {
        console.log("  Three consecutive empty pages — stopping discovery.");
        break;
      }
      continue;
    }

    emptyPageStreak = 0;
    allEntries.push(...entries);
    console.log(
      `    Found ${entries.length} entries (total: ${allEntries.length})`,
    );
  }

  // Deduplicate by URL
  const seenUrls = new Set<string>();
  const uniqueEntries = allEntries.filter((e) => {
    if (seenUrls.has(e.url)) return false;
    seenUrls.add(e.url);
    return true;
  });

  console.log(
    `\nDiscovered ${uniqueEntries.length} unique decision URLs (from ${allEntries.length} total)`,
  );

  // Filter already-processed URLs (for --resume)
  const entriesToProcess = resume
    ? uniqueEntries.filter((e) => !processedSet.has(e.url))
    : uniqueEntries;

  console.log(`URLs to process: ${entriesToProcess.length}`);
  if (resume && uniqueEntries.length !== entriesToProcess.length) {
    console.log(
      `  Skipping ${uniqueEntries.length - entriesToProcess.length} already-processed URLs`,
    );
  }

  if (entriesToProcess.length === 0) {
    console.log("Nothing to process. Exiting.");
    return;
  }

  // ---- Step 2: Initialise database (unless dry run) ----
  let db: Database.Database | null = null;
  let stmts: ReturnType<typeof prepareStatements> | null = null;

  if (!dryRun) {
    db = initDb();
    stmts = prepareStatements(db);
  }

  // ---- Step 3: Fetch and parse each decision detail page ----
  console.log("\nStep 2: Processing individual decision pages...\n");

  let decisionsIngested = state.decisionsIngested;
  let mergersIngested = state.mergersIngested;
  let errors = 0;
  let skipped = 0;
  const sectorCounts: SectorAccumulator = {};

  for (let i = 0; i < entriesToProcess.length; i++) {
    const entry = entriesToProcess[i]!;
    const progress = `[${i + 1}/${entriesToProcess.length}]`;

    console.log(`${progress} Fetching: ${entry.url}`);

    const html = await rateLimitedFetch(entry.url);
    if (!html) {
      console.log(`  SKIP — could not fetch`);
      state.errors.push(`fetch_failed: ${entry.url}`);
      errors++;
      continue;
    }

    try {
      const { decision, merger } = parseDetailPage(html, entry.url, entry);

      if (decision) {
        if (dryRun) {
          console.log(
            `  DECISION: ${decision.case_number} — ${decision.title.slice(0, 80)}`,
          );
          console.log(
            `    type=${decision.type}, sector=${decision.sector}, outcome=${decision.outcome}, fine=${decision.fine_amount}`,
          );
        } else {
          const stmt = force
            ? stmts!.upsertDecision
            : stmts!.insertDecision;
          stmt.run(
            decision.case_number,
            decision.title,
            decision.date,
            decision.type,
            decision.sector,
            decision.parties,
            decision.summary,
            decision.full_text,
            decision.outcome,
            decision.fine_amount,
            decision.gwb_articles,
            decision.status,
          );
          console.log(`  INSERTED decision: ${decision.case_number}`);
        }

        decisionsIngested++;

        if (decision.sector) {
          if (!sectorCounts[decision.sector]) {
            sectorCounts[decision.sector] = {
              name: decision.sector,
              name_en: null,
              description: null,
              decisionCount: 0,
              mergerCount: 0,
            };
          }
          sectorCounts[decision.sector]!.decisionCount++;
        }
      } else if (merger) {
        if (dryRun) {
          console.log(
            `  MERGER: ${merger.case_number} — ${merger.title.slice(0, 80)}`,
          );
          console.log(
            `    sector=${merger.sector}, outcome=${merger.outcome}, acquiring=${merger.acquiring_party?.slice(0, 40)}`,
          );
        } else {
          const stmt = force
            ? stmts!.upsertMerger
            : stmts!.insertMerger;
          stmt.run(
            merger.case_number,
            merger.title,
            merger.date,
            merger.sector,
            merger.acquiring_party,
            merger.target,
            merger.summary,
            merger.full_text,
            merger.outcome,
            merger.turnover,
          );
          console.log(`  INSERTED merger: ${merger.case_number}`);
        }

        mergersIngested++;

        if (merger.sector) {
          if (!sectorCounts[merger.sector]) {
            sectorCounts[merger.sector] = {
              name: merger.sector,
              name_en: null,
              description: null,
              decisionCount: 0,
              mergerCount: 0,
            };
          }
          sectorCounts[merger.sector]!.mergerCount++;
        }
      } else {
        console.log(`  SKIP — could not extract structured data`);
        skipped++;
      }

      // Mark URL as processed
      processedSet.add(entry.url);
      state.processedUrls.push(entry.url);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.error(`  ERROR: ${message}`);
      state.errors.push(`parse_error: ${entry.url}: ${message}`);
      errors++;
    }

    // Save state periodically (every 25 URLs)
    if ((i + 1) % 25 === 0) {
      state.decisionsIngested = decisionsIngested;
      state.mergersIngested = mergersIngested;
      saveState(state);
      console.log(`  [checkpoint] State saved after ${i + 1} URLs`);
    }
  }

  // ---- Step 4: Update sector counts ----
  if (!dryRun && db && stmts) {
    const sectorMeta: Record<string, { name: string; name_en: string }> = {
      energie: {
        name: "Énergie / Energie",
        name_en: "Energy",
      },
      distribution_alimentaire: {
        name: "Distribution Alimentaire / Voedseldistributie",
        name_en: "Food Distribution",
      },
      telecommunications: {
        name: "Télécommunications / Telecommunicatie",
        name_en: "Telecommunications",
      },
      sante: {
        name: "Santé / Gezondheidszorg",
        name_en: "Healthcare",
      },
      banques: {
        name: "Banques et Services Financiers / Banken en Financiële Diensten",
        name_en: "Banking and Financial Services",
      },
      construction: {
        name: "Construction / Bouw",
        name_en: "Construction",
      },
      transport: {
        name: "Transport / Vervoer",
        name_en: "Transport",
      },
      digital: {
        name: "Économie Numérique / Digitale Economie",
        name_en: "Digital Economy",
      },
      media: {
        name: "Médias / Media",
        name_en: "Media",
      },
      professions_liberales: {
        name: "Professions Libérales / Vrije Beroepen",
        name_en: "Liberal Professions",
      },
      automobile: {
        name: "Automobile / Auto",
        name_en: "Automotive",
      },
      chimie: {
        name: "Chimie / Chemie",
        name_en: "Chemicals",
      },
      retail: {
        name: "Commerce de Détail / Kleinhandel",
        name_en: "Retail",
      },
      agriculture: {
        name: "Agriculture / Landbouw",
        name_en: "Agriculture",
      },
    };

    // Count decisions and mergers per sector from the database
    const decisionSectorCounts = db
      .prepare(
        "SELECT sector, COUNT(*) as cnt FROM decisions WHERE sector IS NOT NULL GROUP BY sector",
      )
      .all() as Array<{ sector: string; cnt: number }>;
    const mergerSectorCounts = db
      .prepare(
        "SELECT sector, COUNT(*) as cnt FROM mergers WHERE sector IS NOT NULL GROUP BY sector",
      )
      .all() as Array<{ sector: string; cnt: number }>;

    const finalSectorCounts: Record<
      string,
      { decisions: number; mergers: number }
    > = {};
    for (const row of decisionSectorCounts) {
      if (!finalSectorCounts[row.sector])
        finalSectorCounts[row.sector] = { decisions: 0, mergers: 0 };
      finalSectorCounts[row.sector]!.decisions = row.cnt;
    }
    for (const row of mergerSectorCounts) {
      if (!finalSectorCounts[row.sector])
        finalSectorCounts[row.sector] = { decisions: 0, mergers: 0 };
      finalSectorCounts[row.sector]!.mergers = row.cnt;
    }

    const updateSectors = db.transaction(() => {
      for (const [id, counts] of Object.entries(finalSectorCounts)) {
        const info = sectorMeta[id];
        stmts!.upsertSector.run(
          id,
          info?.name ?? id,
          info?.name_en ?? null,
          null,
          counts.decisions,
          counts.mergers,
        );
      }
    });
    updateSectors();

    console.log(
      `\nUpdated ${Object.keys(finalSectorCounts).length} sector records`,
    );
  }

  // ---- Step 5: Final state save ----
  state.decisionsIngested = decisionsIngested;
  state.mergersIngested = mergersIngested;
  saveState(state);

  // ---- Step 6: Summary ----
  if (!dryRun && db) {
    const decisionCount = (
      db.prepare("SELECT count(*) as cnt FROM decisions").get() as {
        cnt: number;
      }
    ).cnt;
    const mergerCount = (
      db.prepare("SELECT count(*) as cnt FROM mergers").get() as {
        cnt: number;
      }
    ).cnt;
    const sectorCount = (
      db.prepare("SELECT count(*) as cnt FROM sectors").get() as {
        cnt: number;
      }
    ).cnt;

    console.log("\n=== Ingestion Complete ===");
    console.log(`  Decisions in DB:  ${decisionCount}`);
    console.log(`  Mergers in DB:    ${mergerCount}`);
    console.log(`  Sectors in DB:    ${sectorCount}`);
    console.log(`  New decisions:    ${decisionsIngested}`);
    console.log(`  New mergers:      ${mergersIngested}`);
    console.log(`  Errors:           ${errors}`);
    console.log(`  Skipped:          ${skipped}`);
    console.log(`  State saved to:   ${STATE_FILE}`);

    db.close();
  } else {
    console.log("\n=== Dry Run Complete ===");
    console.log(`  Decisions found:  ${decisionsIngested}`);
    console.log(`  Mergers found:    ${mergersIngested}`);
    console.log(`  Errors:           ${errors}`);
    console.log(`  Skipped:          ${skipped}`);
  }

  console.log("\nDone.");
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
