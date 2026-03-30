# Data Procurement Roadmap

This document describes additional datasets that would improve the owner-occupation evidence base, their expected impact, cost, and how to obtain them.

## Priority 1: AddressBase Core (Ordnance Survey)

**What it provides:** Maps every UPRN to a full postal address for 33M+ addressable locations in Great Britain. Enables reliable cross-dataset linking without fuzzy address matching.

**Expected impact:** Could increase the property match rate from 53% to 80%+. Would eliminate most false positive matches (different properties at similar addresses) and false negatives (same property with different address formats).

**Current workaround:** Fuzzy word-level Jaccard address matching with building-number guard at 60% threshold. EPC UPRNs (97.6% coverage) used for within-EPC deduplication.

**How to obtain:**
- Free for public sector bodies under the Public Sector Geospatial Agreement (PSGA)
- Apply at: https://www.ordnancesurvey.co.uk/customers/public-sector/public-sector-geospatial-agreement
- Alternatively, OS Open UPRN (free, OGL licensed) provides UPRNs with coordinates but NOT addresses: https://osdatahub.os.uk/downloads/open/OpenUPRN

**Cost:** Free (PSGA) or commercial licence.

**Priority:** HIGH

---

## Priority 2: Electoral Register (Open Register)

**What it provides:** Names of adults registered to vote at each address. Direct evidence of occupancy by named individuals.

**Expected impact:** Would resolve most uncertain cases. A named individual at a non-corporate address is strong evidence of owner-occupation. Absence from the register at a corporate-owned property confirms non-owner-occupation.

**Current workaround:** EPC tenure field and CCOD/OCOD corporate ownership as indirect signals.

**How to obtain:**
- The Open Register (opt-in portion) is available from local Electoral Registration Officers (EROs)
- Each billing authority has an ERO; contact individually
- For bulk national coverage, explore data sharing with the Cabinet Office or Individual Electoral Registration (IER) digital service
- Template request: "Under the Representation of the People (England and Wales) Regulations 2001, Schedule 1, I request a copy of the full/open register for [authority] in electronic format for research purposes."

**Cost:** Free (Open Register) but fragmented across 380+ EROs. Consider starting with key boroughs: Westminster, Kensington & Chelsea, Camden, Wandsworth.

**Priority:** HIGH if bulk access achievable

---

## Priority 3: Companies House Bulk Data

**What it provides:** Company name, registration number, status (active/dissolved/liquidation), SIC codes, registered address, incorporation date.

**Expected impact:** Would confirm CCOD company classifications and identify dissolved companies (property may have reverted to individual ownership). SIC codes could distinguish property investment companies from operating businesses.

**Current workaround:** CCOD Proprietorship Category field provides structured classification for all matched properties.

**How to obtain:**
- Free bulk download: https://download.companieshouse.gov.uk/en_output.html
- Format: CSV (468MB as single ZIP, or 7-part split)
- Updated monthly
- Key fields: CompanyName, CompanyNumber, CompanyStatus, CompanyCategory, SICCode.SicText_1

**Cost:** Free

**Priority:** LOW-MEDIUM (CCOD structured fields already handle most classification; main value is detecting dissolved companies)

---

## Priority 4: National Polygon Service — Title Number to UPRN Mapping

**What it provides:** Maps every Land Registry title number to a UPRN. Enables direct chaining: CCOD Title Number -> UPRN -> EPC/AddressBase.

**Expected impact:** Would make ownership-to-property matching near-deterministic for registered titles, replacing address-based fuzzy matching entirely.

**Current workaround:** CCOD address matched to PPD address via postcode + fuzzy text matching.

**How to obtain:**
- HM Land Registry National Polygon Service: https://use-land-property-data.service.gov.uk/datasets/nps
- Includes: National Polygon Dataset, Title Descriptor Dataset, Title Number and UPRN Look Up Dataset
- Contact: data.services@mail.landregistry.gov.uk

**Cost:** GBP 20,000/year + VAT (covers all three datasets)

**Priority:** MEDIUM (high value but expensive; consider if project is funded long-term)

---

## Priority 5: Council Tax Property-Level Data (via Data Sharing Agreement)

**What it provides:** Per-property council tax band, discount status (single-person discount, empty-property premium, second-home surcharge), exemption class.

**Expected impact:** Would directly identify occupied vs empty vs second-home properties. Single-person discount is a strong occupancy signal. Empty-property premium confirms vacancy.

**Current workaround:** Aggregate LA-level Band H empty property counts from CTB statistics. No property-level discount data.

**How to obtain:**
- Property-level data is NOT available via FOI (GDPR/personal data)
- Requires a formal Data Sharing Agreement (DSA) with each billing authority under the Digital Economy Act 2017, Part 5
- The agreement must specify a legitimate public interest purpose
- Consider piloting with one willing billing authority (e.g., Westminster, which has the highest Band H concentration)

**Cost:** Administrative only (legal review of DSA template, authority engagement)

**Priority:** MEDIUM (very valuable but complex procurement)

---

## Recommended procurement sequence

| Step | Dataset | Action | Timeline |
|------|---------|--------|----------|
| 1 | Companies House | Download immediately (free) | 1 day |
| 2 | AddressBase Core | Apply for PSGA access | 2-4 weeks |
| 3 | Electoral Register | Contact EROs for Westminster, K&C, Camden | 2-6 weeks |
| 4 | NPRN | Budget request if project funded | Budget-dependent |
| 5 | Council Tax DSA | Legal review + pilot authority | 3-6 months |
