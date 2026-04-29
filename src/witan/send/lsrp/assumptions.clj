(ns witan.send.lsrp.assumptions
  (:require [tablecloth.api :as tc]))

(def non-specific-assumptions
  "
 - It is understood that the \"actual number for 2025 calendar year\" refers to the total count of EHCPs at the end of said calendar year i.e. the January census date for the next calendar year")

(def projection-assumptions
  "
 - Projected values are the median of 1000 simulations, as such a summing of median values will not result in the same value as the median of the total
 - A \"baseline\" projection assumes \"all things remain the same\" in terms of rates of transition rates - new EHCPs, ceased EHCPs, movers and stayers
 - No future plans or interventions are factored in to these projections")

(def non-specific-ehcp-projection-assumptions
  (str "
 - Census counts and projections are based on the Spring school census date according to that calendar year, typically falling on the third Thursday of January
 - EHCPs are modelled as a rate of EHCPs by need/setting/NCY against the background EHCP-eligible population (0-25) by age. The background population is derived from the ONS subnational population projections and mid-year estimates
 - Simulations are not explicitly capped by capacity, however constraints that may have influenced recent placement choices will in turn effect the models transition rates"
       non-specific-assumptions
       projection-assumptions))

(def non-specific-age-related-assumptions
  "
 - Due to protective PII measures a CYP's National Curriculum Year is calculated based on their age according to their birth month/year as of August preceding the school year
 - The age group a CYP falls in calculated by the age they turn according to the NCY they're currently in")

(def Total-number-of-EHC-plans-by-age-group-with-estimated-future-projections-assumptions
  (str "5.1"
       non-specific-age-related-assumptions
       non-specific-ehcp-projection-assumptions))

(def mainstream-post-16-specialist-provision-assumptions
  "
 - It is understood that Mainstream Post 16 specialist provision refers to a placement at specialist base within a mainstream setting with post 16 facilities (GFE, Sixth Form, etc.) where a CYP at post 16 age (16-25 years old)")

(def support-&-specialist-base-assumptions
  (str "
 - Evidence of a CYP being placed at a Support base (where places are funded and commissioned by settings/multi-academy trusts) are typically not recorded, as they are not funded or commissioned by the LA. In this case a CYP will be recorded as attending a \"Mainstream school or academy\"
 - Due to Mainstream Post 16 specialist provision, Post 16 CYPs at a Specialist base are not included in counts by \"Specialist bases in mainstream settings\""
       mainstream-post-16-specialist-provision-assumptions))

(def non-maintained-&-indepedent-school-assumptions
  "
 - It is understood that within Non-Maintained Special School and Independent Schools should be included Special and non-special Indepedent schools")

(def Current-and-projected-number-of-all-CYP-with-EHC-plans-by-provision-assumptions
  (str "6.
 - Those \"not in education\" but still holding an EHCP (NEET, NIEO or NIEC) are included in \"Other (including hospital schools where applicable)\""
       support-&-specialist-base-assumptions
       non-maintained-&-indepedent-school-assumptions
       non-specific-ehcp-projection-assumptions))

(def non-specific-ehcp-projections-by-primary-need-assumptions
  "
 - Section 7 asks for \"number of all CYP with EHC plans\", no SEN Support. As such \"SEN support but no specialist assessment of type of need\" as a Primary Need has been calculated as 0")

(def Current-and-projected-number-of-all-CYP-with-EHC-plans-by-primary-need-assumptions
  (str "7."
       non-specific-ehcp-projections-by-primary-need-assumptions
       non-specific-ehcp-projection-assumptions))

(def Current-and-projected-number-of-all-CYP-with-EHC-plans-in-Early-Years-Settings-including-PVIs-by-primary-need-assumptions
  (str "7.1"
       non-specific-ehcp-projections-by-primary-need-assumptions
       non-specific-ehcp-projection-assumptions))

(def Current-and-projected-number-of-all-CYP-with-EHC-plans-in-Mainstream-Schools-or-Academies-including-Support-Bases-by-primary-need
  (str "7.2"
       support-&-specialist-base-assumptions
       non-specific-ehcp-projections-by-primary-need-assumptions
       non-specific-ehcp-projection-assumptions))

(def Current-and-projected-number-of-all-CYP-with-EHC-plans-in-Specialist-Bases-by-primary-need
  (str "7.3"
       support-&-specialist-base-assumptions
       non-specific-ehcp-projections-by-primary-need-assumptions
       non-specific-ehcp-projection-assumptions))

(def Current-and-projected-number-of-all-CYP-with-EHC-plans-in-Maintained-Special-Schools-or-Special-Academies-by-primary-need
  (str "7.4"
       non-specific-ehcp-projections-by-primary-need-assumptions
       non-specific-ehcp-projection-assumptions))

(def Current-and-projected-number-of-all-CYP-with-EHC-plans-in-Non-Maintained-Special-Schools-or-Independent-Schools-by-primary-need
  (str "7.5"
       non-maintained-&-indepedent-school-assumptions
       non-specific-ehcp-projections-by-primary-need-assumptions
       non-specific-ehcp-projection-assumptions))

(def Current-and-projected-number-of-all-CYP-with-EHC-plans-in-Alternative-Provision-or-Hospital-Schools-by-primary-need
  (str "7.6
 - Note that Hospital Schools are here counted together with Alternative Provision, however in section 6 Hospital Schools are counted with \"Other (including hospital schools where applicable)\""
       non-specific-ehcp-projections-by-primary-need-assumptions
       non-specific-ehcp-projection-assumptions))

(def Current-and-projected-number-of-all-CYP-with-EHC-plans-in-Post-16-Further-Education-or-Specialist-Further-Education-Settings-by-primary-need
  (str "7.7
 - It is understood that \"Post-16 (Further Education or Specialist Further Education) Settings\" refers to the previously defined \"Mainstream Post 16 provision\", \"Mainstream Post 16 specialist provision\" and \"Specialist Post-16 institutions\""
       mainstream-post-16-specialist-provision-assumptions
       non-specific-ehcp-projections-by-primary-need-assumptions
       non-specific-ehcp-projection-assumptions))

(def Current-and-projected-number-of-all-EHCNA-requests-by-CYP-age
  (str "10.
 - Counts and projections consist of all request between Spring school census dates
 - EHCNA requests are modelled as a rate of EHCNA requests by NCY against the background EHCP-eligible population (0-25) by age. The background population is derived from the ONS subnational population projections and mid-year estimates
 - EHCNA requests are counted in spite of their outcome; whether they did or did not move to an assessment, or the request was ceased/withdrawn"
       projection-assumptions
       non-specific-assumptions
       non-specific-age-related-assumptions))

(def Current-and-projected-number-of-all-EHC-Needs-Assessments-by-CYP-age
  (str "11.
 - Counts and projections consist of all needs assessments between Spring school census dates
 - EHCNAs are modelled as a rate of EHCNAs by NCY against the background EHCP-eligible population (0-25) by age. The background population is derived from the ONS subnational population projections and mid-year estimates
 - EHCNAs are counted in spite of their outcome; whether they did or did not result in a plan, or were ceased/withdrawn"
       projection-assumptions
       non-specific-assumptions
       non-specific-age-related-assumptions))

(def Current-and-projected-number-of-all-EHCNAs-that-result-in-an-EHCP
  (str "12.
 - Counts and projections consist of all needs assessments resulting in an EHCP between Spring school census dates
 - EHCNAs that become EHCPs are modelled as a rate of EHCNA to EHCP by NCY against the background EHCP-eligible population (0-25) by age. The background population is derived from the ONS subnational population projections and mid-year estimates
 - EHCNAs are counted by positive outcome"
       projection-assumptions
       non-specific-assumptions
       non-specific-age-related-assumptions))

(def assumptions-map
  {"5.1" Total-number-of-EHC-plans-by-age-group-with-estimated-future-projections-assumptions
   "6.0" Current-and-projected-number-of-all-CYP-with-EHC-plans-by-provision-assumptions
   "7.0" Current-and-projected-number-of-all-CYP-with-EHC-plans-by-primary-need-assumptions
   "7.1" Current-and-projected-number-of-all-CYP-with-EHC-plans-in-Early-Years-Settings-including-PVIs-by-primary-need-assumptions
   "7.2" Current-and-projected-number-of-all-CYP-with-EHC-plans-in-Mainstream-Schools-or-Academies-including-Support-Bases-by-primary-need
   "7.3" Current-and-projected-number-of-all-CYP-with-EHC-plans-in-Specialist-Bases-by-primary-need
   "7.4" Current-and-projected-number-of-all-CYP-with-EHC-plans-in-Maintained-Special-Schools-or-Special-Academies-by-primary-need
   "7.5" Current-and-projected-number-of-all-CYP-with-EHC-plans-in-Non-Maintained-Special-Schools-or-Independent-Schools-by-primary-need
   "7.6" Current-and-projected-number-of-all-CYP-with-EHC-plans-in-Alternative-Provision-or-Hospital-Schools-by-primary-need
   "7.7" Current-and-projected-number-of-all-CYP-with-EHC-plans-in-Post-16-Further-Education-or-Specialist-Further-Education-Settings-by-primary-need
   "10.0" Current-and-projected-number-of-all-EHCNA-requests-by-CYP-age
   "11.0" Current-and-projected-number-of-all-EHC-Needs-Assessments-by-CYP-age
   "12.0" Current-and-projected-number-of-all-EHCNAs-that-result-in-an-EHCP})

(defn format-assumptions [assumptions-map]
  (as-> assumptions-map $
    (map (fn [[k v]](assoc {}
                           "Section" (read-string k)
                           "Assumption" v)) $)
    (tc/dataset $ {:dataset-name "Assumptions"})
    (tc/order-by $ "Section")))
