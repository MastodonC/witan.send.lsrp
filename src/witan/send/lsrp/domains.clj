(ns witan.send.lsrp.domains
  (:require [clojure.string :as s]))

;; Calendar years

(def lsrp-calendar-years
  #{2025 2026 2027 2028 2029})

;; remove?
#_(def lsrp-calendar-years+1
    (into (sorted-set) (conj lsrp-calendar-years 2030)))

;; Age Groups

(defn inclusive-range [beginning end]
  (range beginning (inc end)))

(def under-5
  (into (sorted-set) (inclusive-range -5 1)))

(def age-5-10
  (into (sorted-set) (inclusive-range 0 5)))

(def age-11-15
  (into (sorted-set) (inclusive-range 6 10)))

(def age-16-19
  (into (sorted-set) (inclusive-range 11 14)))

(def age-20-25
  (into (sorted-set) (inclusive-range 15 20)))

(defn lsrp-age-group [y]
  (cond
    (under-5 y)   :under-5
    (age-5-10 y)  :age-5-10
    (age-11-15 y) :age-11-15
    (age-16-19 y) :age-16-19
    (age-20-25 y) :age-20-25
    :else :outside-of-send-age))

(def lsrp-age-group-names
  {:under-5   "Under 5"
   :age-5-10  "Age 5 to 10"
   :age-11-15 "Age 11 to 15"
   :age-16-19 "Age 16 to 19"
   :age-20-25 "Age 20 to 25"})

;; Provision

(def lsrp-provision
  ["Early Years settings including PVIs"
   "Mainstream schools or academies"
   "Support bases in mainstream settings"
   "Specialist bases in mainstream settings"
   "Maintained special schools or special academies"
   "NMSS or independent schools - LA funded placements"
   "NMSS or independent schools - other suitable arrangements"
   "Alternative Provision"
   "Mainstream Post 16 provision"
   "Mainstream Post 16 specialist provision"
   "Specialist Post-16 institutions"
   "Elective Home Education (EHE)"
   "Other arrangements by LA (EOTAS)"
   "Other (including hospital schools where applicable)"])

(defn setting->lsrp-provision [setting ncy]
  (cond
    (#{"AP"} setting)
    "Alternative Provision"
    (#{"EHE"} setting)
    "Elective Home Education (EHE)"
    (#{"EOTAS"} setting)
    "Other arrangements by LA (EOTAS)"
    (#{"EYP"} setting)
    "Early Years settings including PVIs"
    (#{"MsIn"} setting)
    "NMSS or independent schools - LA funded placements"
    (s/includes? setting "SpIn")
    "NMSS or independent schools - LA funded placements"
    (s/includes? setting "SpNm")
    "NMSS or independent schools - LA funded placements"
    (s/includes? setting "MsMdA")
    "Mainstream schools or academies"
    (#{"6FC"} setting)
    "Mainstream schools or academies"
    (#{"GFE"} setting)
    "Mainstream Post 16 provision"
    (#{"NEET" "NIEC" "NIEO" "OPA"} setting)
    "Other (including hospital schools where applicable)"
    (and (s/includes? setting "RP")
         (>= 12 ncy))
    "Specialist bases in mainstream settings"
    (and (s/includes? setting "SENU")
         (>= 12 ncy))
    "Specialist bases in mainstream settings"
    (and (s/includes? setting "SENU")
         (<= 12 ncy))
    "Mainstream Post 16 specialist provision"
    (and (s/includes? setting "RP")
         (<= 12 ncy))
    "Mainstream Post 16 specialist provision"
    (#{"SP16"} setting)
    "Specialist Post-16 institutions"
    (s/includes? setting "SpMdA")
    "Maintained special schools or special academies"))

;; Primary needs

(def lsrp-needs
  ["Autistic Spectrum Disorder"
   "Hearing Impairment"
   "Moderate Learning Difficulty"
   "Multi- Sensory Impairment"
   "Physical Disability"
   "Profound & Multiple Learning Difficulty"
   "Social, Emotional and Mental Health"
   "Speech, Language and Communications needs"
   "Severe Learning Difficulty"
   "Specific Learning Difficulty"
   "Visual Impairment"
   "Other Difficulty/Disability"
   "SEN support but no specialist assessment of type of need"])

(def need->lsrp-need
  {"ASD" "Autistic Spectrum Disorder"
   "HI" "Hearing Impairment"
   "MLD" "Moderate Learning Difficulty"
   "MSI" "Multi- Sensory Impairment"
   "PD" "Physical Disability"
   "PMLD" "Profound & Multiple Learning Difficulty"
   "SEMH" "Social, Emotional and Mental Health"
   "SLCN" "Speech, Language and Communications needs"
   "SLD" "Severe Learning Difficulty"
   "SPLD" "Specific Learning Difficulty"
   "VI" "Visual Impairment"
   "OTH" "Other Difficulty/Disability"
   nil "SEN support but no specialist assessment of type of need"})
