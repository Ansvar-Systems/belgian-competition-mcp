/**
 * Seed the BCA database with sample decisions, mergers, and sectors.
 *
 * Usage:
 *   npx tsx scripts/seed-sample.ts
 *   npx tsx scripts/seed-sample.ts --force
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["BCA_DB_PATH"] ?? "data/bca.db";
const force = process.argv.includes("--force");

const dir = dirname(DB_PATH);
if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
if (force && existsSync(DB_PATH)) { unlinkSync(DB_PATH); console.log(`Deleted ${DB_PATH}`); }

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");
db.exec(SCHEMA_SQL);
console.log(`Database initialised at ${DB_PATH}`);

interface SectorRow { id: string; name: string; name_en: string; description: string; decision_count: number; merger_count: number; }

const sectors: SectorRow[] = [
  { id: "energie", name: "Énergie / Energie", name_en: "Energy", description: "Belgian energy market including electricity (Elia TSO) and gas (Fluxys TSO). Liberalised retail market with Engie, Luminus, Eneco. BCA coordinates with sector regulator CREG/VREG on competition matters.", decision_count: 22, merger_count: 14 },
  { id: "distribution_alimentaire", name: "Distribution Alimentaire / Voedseldistributie", name_en: "Food Distribution", description: "Belgian food retail dominated by Colruyt, Delhaize, Carrefour, and Lidl/Aldi. BCA has active enforcement on supplier relations and vertical restraints. CEL/WER Book IV applies.", decision_count: 25, merger_count: 11 },
  { id: "telecommunications", name: "Télécommunications / Telecommunicatie", name_en: "Telecommunications", description: "Belgian telecom market with three MNOs (Proximus, Orange Belgium, Telenet/Base). BCA coordinates with sector regulator BIPT/IBPT. Focus on broadband competition and cable monopoly.", decision_count: 18, merger_count: 9 },
  { id: "sante", name: "Santé / Gezondheidszorg", name_en: "Healthcare", description: "Belgian pharmaceutical market, hospital sector, and medical devices. BCA has investigated pharmaceutical pay-for-delay agreements and hospital merger control.", decision_count: 12, merger_count: 8 },
  { id: "banques", name: "Banques et Services Financiers / Banken en Financiële Diensten", name_en: "Banking and Financial Services", description: "Belgian banking sector: BNP Paribas Fortis, KBC, Belfius, ING Belgium. BCA has investigated interchange fees and payment systems competition.", decision_count: 10, merger_count: 6 },
];

const insertSector = db.prepare("INSERT OR IGNORE INTO sectors (id, name, name_en, description, decision_count, merger_count) VALUES (?, ?, ?, ?, ?, ?)");
for (const s of sectors) insertSector.run(s.id, s.name, s.name_en, s.description, s.decision_count, s.merger_count);
console.log(`Inserted ${sectors.length} sectors`);

interface DecisionRow { case_number: string; title: string; date: string; type: string; sector: string; parties: string; summary: string; full_text: string; outcome: string; fine_amount: number | null; gwb_articles: string; status: string; }

const decisions: DecisionRow[] = [
  {
    case_number: "ABC-2023-I/O-03",
    title: "Proximus — Abus de position dominante dans les services internet B2B",
    date: "2023-09-12",
    type: "abuse_of_dominance",
    sector: "telecommunications",
    parties: "Proximus SA",
    summary: "L'Autorité belge de la Concurrence a infligé une amende de EUR 35 millions à Proximus pour abus de position dominante sur le marché des services internet à haut débit pour entreprises. Proximus avait refusé de fournir un accès dégroupé à des tarifs raisonnables aux concurrents.",
    full_text: "ABC-2023-I/O-03 Proximus abus position dominante B2B. Faits: Proximus (ancien Belgacom), opérateur historique avec SMP sur les marchés de gros, a pratiqué une compression de marges (margin squeeze) sur le marché des services internet entreprises. Les concurrents (Telenet Bedrijfsoplossingen, Orange Belgium Business) ne pouvaient pas reproduire les offres de détail de Proximus avec les tarifs de gros pratiqués. Test AEEP (Equally Efficient Competitor): Proximus ne pouvait pas répliquer ses propres offres de détail avec les prix de gros qu'il facturait. Articles WER: Art. IV.2 §1 WER (abus de position dominante); Art. 102 TFUE. Coordination avec IBPT/BIPT (régulateur sectoriel). Amende: EUR 35 millions. Conditions: tarifs de gros révisés; accès non-discriminatoire; monitoring IBPT 3 ans.",
    outcome: "fine",
    fine_amount: 35000000,
    gwb_articles: "Art. IV.2 §1 WER, Art. 102 TFUE",
    status: "final",
  },
  {
    case_number: "ABC-2022-I/O-15",
    title: "Cartel dans la distribution de médicaments — Pay-for-delay",
    date: "2022-11-08",
    type: "cartel",
    sector: "sante",
    parties: "Laboratoire pharmaceutique A (anonymisé); Fabricant générique B (anonymisé)",
    summary: "L'ABC a infligé des amendes de EUR 12 millions à deux sociétés pharmaceutiques pour un accord pay-for-delay: le fabricant de médicaments originaux avait versé des compensations à un fabricant de génériques pour retarder l'entrée de génériques moins chers sur le marché belge.",
    full_text: "ABC-2022-I/O-15 Pay-for-delay médicaments. Contexte: accord de règlement de litige en matière de brevets entre un laboratoire pharmaceutique (médicament original) et un fabricant de génériques. Le laboratoire a versé une valeur substantielle (compensations financières et licence exclusive) au fabricant de génériques contre l'engagement de ne pas entrer sur le marché belge pendant 3 ans. Qualification juridique: la Cour de justice UE (Lundbeck, Paroxetine) confirme que de tels accords constituent une restriction de concurrence par objet lorsque le paiement inverse excède les frais légaux et pousse le génériciste à rester hors marché. Article WER: Art. IV.1 §1 WER (accords anticoncurrentiels); Art. 101 TFUE. Préjudice: retard de 3 ans pour l'entrée de génériques (économies potentielles pour l'INAMI estimées à EUR 45M). Amendes: fabricant original EUR 9M; fabricant générique EUR 3M. Demande de clémence partielle accordée.",
    outcome: "fine",
    fine_amount: 12000000,
    gwb_articles: "Art. IV.1 §1 WER, Art. 101 TFUE",
    status: "final",
  },
  {
    case_number: "ABC-2024-I/O-01",
    title: "Colruyt — Restriction de ventes en ligne et prix imposés",
    date: "2024-01-30",
    type: "abuse_of_dominance",
    sector: "distribution_alimentaire",
    parties: "Colruyt Group SA; Colis Privé BVBA",
    summary: "L'ABC a ouvert une enquête formelle contre Colruyt pour restrictions verticales dans ses relations avec des fournisseurs de produits alimentaires: pratiques de prix de revente imposés (RPM) et restrictions de ventes en ligne dans les contrats fournisseurs. Enquête en cours.",
    full_text: "ABC-2024-I/O-01 Colruyt restrictions verticales. Marchés concernés: distribution alimentaire au détail en Belgique (Colruyt avec ~30% de parts de marché national); e-commerce alimentaire (Collect&Go). Pratiques examinées: (1) prix de revente imposés (RPM): clauses dans contrats fournisseurs fixant le prix minimum de revente, y compris pour les ventes en ligne; (2) restrictions de vente en ligne: interdiction pour les fournisseurs de vendre directement aux consommateurs belges via internet certaines gammes de produits; (3) MFN clauses: clauses de nation la plus favorisée empêchant fournisseurs d'offrir de meilleures conditions à des concurrents. Cadre juridique: Art. IV.1 §1 WER; Règlement (UE) 2022/720 (règlement d'exemption verticale). Enquête formelle ouverte: notification aux parties; accès au dossier; audience prévue. Statut: en cours.",
    outcome: "cleared",
    fine_amount: null,
    gwb_articles: "Art. IV.1 §1 WER, Art. 101 TFUE",
    status: "pending",
  },
  {
    case_number: "ABC-2022-I/O-08",
    title: "Interchange bancaire — Coordination entre banques belges sur les frais",
    date: "2022-06-20",
    type: "cartel",
    sector: "banques",
    parties: "Febelfin (Fédération belge du secteur financier); BNP Paribas Fortis; KBC; Belfius; ING Belgique",
    summary: "L'ABC a sanctionné Febelfin et quatre grandes banques belges pour coordination des frais d'interchange multilatéraux pour les paiements par carte de débit au-delà du niveau autorisé par le Règlement IFR. Amende totale EUR 18 millions.",
    full_text: "ABC-2022-I/O-08 Interchange bancaire Belgique. Contexte réglementaire: le Règlement (UE) 2015/751 (IFR — Interchange Fee Regulation) plafonne les commissions d'interchange pour cartes de débit (0,2%) et de crédit (0,3%). Faits: les banques ont, via Febelfin, coordonné le maintien de frais supplémentaires (net compensation fees) s'ajoutant à l'interchange réglementé, contournant les plafonds IFR. Durée: 2016-2021 (depuis l'entrée en vigueur de l'IFR). Qualification: Art. IV.1 §1 WER — coordination horizontale entre concurrents; absence d'exemption possible car restriction caractérisée. Amendes: Febelfin EUR 2M; BNP Paribas Fortis EUR 5,5M; KBC EUR 4,8M; Belfius EUR 3,2M; ING EUR 2,5M. Recours: les banques ont contesté devant la Cour d'appel de Bruxelles.",
    outcome: "fine",
    fine_amount: 18000000,
    gwb_articles: "Art. IV.1 §1 WER, Art. 101 TFUE",
    status: "final",
  },
  {
    case_number: "ABC-2023-I/O-12",
    title: "Enquête sectorielle — Marché résidentiel de l'énergie en Belgique",
    date: "2023-07-05",
    type: "sector_inquiry",
    sector: "energie",
    parties: "Marché belge de l'énergie (Engie, Luminus, Eneco et autres)",
    summary: "L'ABC a publié son rapport d'enquête sectorielle sur le marché résidentiel de l'énergie. Résultat principal: la structure oligopolistique du marché facilite la coordination implicite des prix. Recommandations: transparence accrue des tarifs, portabilité facilitée, accès simplifié aux données de consommation (smart meters).",
    full_text: "ABC-2023-I/O-12 Enquête sectorielle énergie résidentielle. Structure de marché: Top-3 (Engie, Luminus, Eneco) détiennent >75% du marché résidentiel; HHI national >2500; différences régionales notables (Wallonie plus concentrée). Problèmes identifiés: (1) Coordination implicite des prix: parallélisme tarifaire lors des hausses de prix (énergie 2021-2023); signalisation par annonces publiques; (2) Switching costs: changement de fournisseur complexe (15 jours minimum); pénalités de résiliation fréquentes; (3) Comparabilité difficile: offres variables, bonus d'entrée, structures tarifaires incompatibles; (4) Smart meters: accès aux données de consommation limité pour les concurrents. Recommandations à la CREG et au gouvernement fédéral: portail officiel de comparaison des tarifs; accès standardisé aux données smart meter; simplification du switching (5 jours); obligation de proposer une offre fixe et transparente. BCA: monitoring des prix post-rapport; possibles enquêtes sur entreprises spécifiques.",
    outcome: "cleared",
    fine_amount: null,
    gwb_articles: "Art. IV.45 WER (enquête sectorielle)",
    status: "final",
  },
];

const insertDecision = db.prepare(`INSERT OR IGNORE INTO decisions (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, gwb_articles, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
for (const d of decisions) insertDecision.run(d.case_number, d.title, d.date, d.type, d.sector, d.parties, d.summary, d.full_text, d.outcome, d.fine_amount, d.gwb_articles, d.status);
console.log(`Inserted ${decisions.length} decisions`);

interface MergerRow { case_number: string; title: string; date: string; sector: string; acquiring_party: string; target: string; summary: string; full_text: string; outcome: string; turnover: number | null; }

const mergers: MergerRow[] = [
  {
    case_number: "ABC-2023-C/C-04",
    title: "Delhaize Group / Bio-Planet et Tom & Co (réseau de magasins)",
    date: "2023-04-18",
    sector: "distribution_alimentaire",
    acquiring_party: "Delhaize Group NV",
    target: "Bio-Planet SA; Tom & Co NV (chaînes de magasins spécialisés)",
    summary: "L'ABC a autorisé en phase 1 l'acquisition par Delhaize Group des chaînes de magasins spécialisés Bio-Planet (alimentation biologique) et Tom & Co (animaleries). Pas de chevauchements significatifs sur les marchés pertinents avec l'activité principale de Delhaize.",
    full_text: "ABC-2023-C/C-04 Delhaize / Bio-Planet et Tom & Co. Transaction: acquisition simultanée de deux chaînes de niche. Bio-Planet (28 magasins, alimentation biologique): marché distinct de la distribution alimentaire généraliste; aucun chevauchement significatif avec supermarchés Delhaize. Tom & Co (65 magasins, animaleries): marché des animaleries et accessoires pour animaux; Delhaize sans présence dans ce segment. Analyse: (1) Distribution alimentaire généraliste: Delhaize présent, Bio-Planet absent — pas de chevauchement; (2) Distribution alimentaire bio: nouveaux entrants fréquents; Delhaize a un rayon bio dans ses supermarchés mais segments distincts; (3) Animaleries: marché distinct; Delhaize sans présence. Seuils de notification: chiffres d'affaires combinés en Belgique >EUR 150M. Autorisation Phase 1 sans conditions.",
    outcome: "cleared_phase1",
    turnover: 650000000,
  },
  {
    case_number: "ABC-2022-C/C-11",
    title: "Telenet / SFR Belgium — Acquisition câble et MVNO",
    date: "2022-09-29",
    sector: "telecommunications",
    acquiring_party: "Telenet Group NV",
    target: "SFR Belgium SA (opérateur virtuel mobile et services câble Wallonie)",
    summary: "L'ABC a autorisé l'acquisition de SFR Belgium par Telenet avec conditions. La transaction combinait le réseau câble de Telenet en Flandre avec les activités MVNO de SFR en Wallonie. Conditions: accès de gros ouvert aux MVNO tiers sur le réseau combiné à des tarifs régulés.",
    full_text: "ABC-2022-C/C-11 Telenet / SFR Belgium. Transaction: Telenet acquiert SFR Belgium (MVNO sur réseau Proximus + accords commerciaux réseau câble Wallonie). Telenet: câblo-opérateur dominant en Flandre (>55% pénétration), propriétaire réseau cable Telenet/BASE. SFR Belgium: MVNO mobile; environ 400.000 clients mobiles en Belgique. Marchés concernés: (1) fourniture en gros d'accès mobile (Telenet+SFR: 3→2 MVNO acheteurs majeurs sur réseau Proximus); (2) fourniture de détail de services mobiles (concentration horizontale); (3) services internet résidentiel (Wallonie: extension géographique Telenet via SFR). Conditions: (1) accès MVNO de gros sur réseau combiné à tarifs BIPT-régulés pour 3 MVNO tiers minimum pendant 7 ans; (2) maintien des tarifs SFR en Wallonie 3 ans; (3) interopérabilité réseau câble Wallonie-Flandre. Monitoring BIPT.",
    outcome: "cleared_with_conditions",
    turnover: 3100000000,
  },
  {
    case_number: "ABC-2024-C/C-02",
    title: "ING Belgique / Record Bank (activités bancaires retail)",
    date: "2024-03-05",
    sector: "banques",
    acquiring_party: "ING Belgique SA",
    target: "Record Bank SA (réseau d'agents bancaires indépendants)",
    summary: "L'ABC a autorisé en phase 1 l'intégration des activités de Record Bank (réseau d'agents bancaires) dans ING Belgique. Pas d'obstacles significatifs à la concurrence identifiés. La transaction s'inscrit dans la consolidation du secteur bancaire belge.",
    full_text: "ABC-2024-C/C-02 ING Belgique / Record Bank. Transaction: ING Belgique intègre les activités de banque de détail de Record Bank (filiale de ING NV) — env. 100 agents bancaires indépendants, 350.000 clients, EUR 12 milliards d'actifs sous gestion. Context: Record Bank était déjà détenue à 100% par ING NV; restructuration interne visant à unifier marque et infrastructure. Marchés: banque de détail résidentielle en Belgique; crédits hypothécaires; épargne et placements. Parts de marché post-fusion: ING (combined) ~15% en banque de détail — en deçà du seuil de dominance (30%). Pas de chevauchements nouveaux (Record Bank déjà contrôlée par ING). Autorisation Phase 1.",
    outcome: "cleared_phase1",
    turnover: 4500000000,
  },
];

const insertMerger = db.prepare(`INSERT OR IGNORE INTO mergers (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
for (const m of mergers) insertMerger.run(m.case_number, m.title, m.date, m.sector, m.acquiring_party, m.target, m.summary, m.full_text, m.outcome, m.turnover);
console.log(`Inserted ${mergers.length} mergers`);

const dc = (db.prepare("SELECT COUNT(*) as n FROM decisions").get() as { n: number }).n;
const mc = (db.prepare("SELECT COUNT(*) as n FROM mergers").get() as { n: number }).n;
const sc = (db.prepare("SELECT COUNT(*) as n FROM sectors").get() as { n: number }).n;
console.log(`\nDatabase summary:\n  Decisions: ${dc}\n  Mergers: ${mc}\n  Sectors: ${sc}\n\nSeed complete.`);
