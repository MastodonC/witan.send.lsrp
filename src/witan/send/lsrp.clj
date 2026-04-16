(ns witan.send.lsrp
  (:require [witan.send.adroddiad.analysis.total-domain :as td]
            [witan.send.adroddiad.transitions :as tr]
            [witan.send :as ws]
            [tablecloth.api :as tc]
            [ham-fisted.reduce :as hf-reduce]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.dataset.reductions :as ds-reduce]
            [clojure.string :as s]))

(def lsrp-calendar-years
  #{2025 2026 2027 2028 2029})

(defn read-simulation-data [config-path sim-prefix]
  (td/simulation-data-from-config config-path sim-prefix))

(defn historic-transition-counts [transitions-path]
  (td/historic-ehcp-count (tc/dataset transitions-path {:key-fn keyword})))

;; 5. Current and projected number of all CYP with EHC plans or receiving top ups by age
;; 5.1 Total number of EHC plans by age group (with estimated future projections)

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

(def lsrp-provision
  #{"Early Years settings including PVIs"
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
    "Other (including hospital schools where applicable)"})

;; can't write the default setting->lsrp-provision until I resolve which settings translate to which provision and whether I need to consider NCY
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
    "NMSS or independent schools - LA funded placements" ;; assumed LA funded not "NMSS or independent schools - other suitable arrangements"
    (s/includes? setting "SpIn")
    "NMSS or independent schools - LA funded placements" ;; assumed LA funded not "NMSS or independent schools - other suitable arrangements"
    (s/includes? setting "SpNm")
    "NMSS or independent schools - LA funded placements" ;; assumed LA funded not "NMSS or independent schools - other suitable arrangements"
    (and (s/includes? setting "MsMdA")
         ((into (sorted-set) (range 0 12)) ncy))
    "Mainstream schools or academies"
    (and (s/includes? setting "MsMdA")
         (<= 12 ncy))
    "Mainstream Post 16 provision"
    (#{"6FC" "GFE"} setting)
    "Mainstream Post 16 provision"
    (#{"NEET" "NIEC" "NIEO" "OPA"} setting)
    "Other (including hospital schools where applicable)"
    (s/includes? setting "RP")
    "Specialist bases in mainstream settings" ;; assummed LA funded not "Support bases in mainstream settings"
    (s/includes? setting "SENU")
    "Specialist bases in mainstream settings" ;; assummed LA funded not "Support bases in mainstream settings"
    (#{"SP16"} setting)
    "Specialist Post-16 institutions"
    (s/includes? setting "SpMdA")
    "Maintained special schools or special academies" ;; don't understand where "Mainstream Post 16 specialist provision" should go
    ))

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

(defn ->empty-ds [domains key]
  (tc/dataset
   (map (fn [v] (assoc (reduce (fn [m k] (assoc m k 0.0)) {} lsrp-calendar-years) key v)) domains)))

(defn summarise
  [simulation-results
   {:keys [historic-transitions-count simulation-count domain-key
           numerator-grouping-keys denominator-grouping-keys transform-simulation-f]
    :or {numerator-grouping-keys (let [cy-base [:calendar-year]]
                                   (if (keyword? domain-key)
                                     (conj cy-base domain-key)
                                     (into cy-base domain-key)))
         denominator-grouping-keys [:calendar-year]
         transform-simulation-f td/transform-simulation}
    :as config}]
  (let [summary
        (tc/order-by
         (->> simulation-results
              (hf-reduce/preduce
               ;; init-val
               (fn [] [])
               ;; rfn
               (fn [acc sim]
                 (conj acc
                       (try
                         (transform-simulation-f
                          sim
                          (assoc config
                                 :denominator-grouping-keys denominator-grouping-keys
                                 :historic-transitions-count historic-transitions-count
                                 :numerator-grouping-keys numerator-grouping-keys
                                 :domain-key domain-key))
                         (catch Exception e (throw (ex-info "Failed to transform simulation."
                                                            {:sim sim
                                                             :denominator-grouping-keys denominator-grouping-keys
                                                             :historic-transitions-count historic-transitions-count
                                                             :numerator-grouping-keys numerator-grouping-keys}
                                                            e))))))
               ;; merge-fn
               (fn [acc acc']
                 (into acc acc')))
              (ds-reduce/group-by-column-agg
               numerator-grouping-keys
               {:transition-count-summary
                (td/percentiles-reducer simulation-count :transition-count)}))
         numerator-grouping-keys)]
    (-> summary
        (tc/select-columns (conj numerator-grouping-keys :transition-count-summary))
        (tc/separate-column :transition-count-summary :infer identity))))

(defn transform-age-group-simulation
  [sim {:keys [numerator-grouping-keys denominator-grouping-keys historic-transitions-count]}]
  (let [census (-> (tc/concat-copying historic-transitions-count sim)
                   (tr/transitions->census))
        denominator (-> census
                        (tc/group-by denominator-grouping-keys)
                        (tc/aggregate {:denominator #(dfn/sum (:transition-count %))}))]
    (as-> census $
      (tc/map-columns $ :age-group [:academic-year]
                      (fn [ncy] (lsrp-age-group-names
                                 (lsrp-age-group ncy))))
      (tc/group-by $ numerator-grouping-keys)
      (tc/aggregate $ {:transition-count #(dfn/sum (:transition-count %))})
      (tc/group-by $ :age-group {:result-type :as-seq})
      (map #(td/add-diff % :transition-count) $)
      (apply tc/concat $)
      (tc/rename-columns $
                         {:diff :ehcp-diff
                          :pct-diff :ehcp-pct-diff})
      (tc/inner-join $ denominator denominator-grouping-keys)
      (tc/map-columns $ :pct-ehcps [:transition-count :denominator] #(dfn// %1 %2))
      (tc/order-by $ numerator-grouping-keys))))

(defn age-group-summaries [config-path sim-prefix transitions-path]
  (summarise (read-simulation-data config-path sim-prefix)
             {:domain-key :age-group
              :historic-transitions-count (historic-transition-counts transitions-path)
              :simulation-count (get-in (ws/read-config config-path) [:projection-parameters :simulations])
              :transform-simulation-f transform-age-group-simulation}))

(defn transform-need-simulation
  [sim {:keys [numerator-grouping-keys denominator-grouping-keys historic-transitions-count]}]
  (let [census (-> (tc/concat-copying historic-transitions-count sim)
                   (tr/transitions->census))
        denominator (-> census
                        (tc/group-by denominator-grouping-keys)
                        (tc/aggregate {:denominator #(dfn/sum (:transition-count %))}))]
    (as-> census $
      (tc/map-columns $ :need [:need]
                      (fn [need] (need->lsrp-need need)))
      (tc/group-by $ numerator-grouping-keys)
      (tc/aggregate $ {:transition-count #(dfn/sum (:transition-count %))})
      (tc/group-by $ :need {:result-type :as-seq})
      (map #(td/add-diff % :transition-count) $)
      (apply tc/concat $)
      (tc/rename-columns $
                         {:diff :ehcp-diff
                          :pct-diff :ehcp-pct-diff})
      (tc/inner-join $ denominator denominator-grouping-keys)
      (tc/map-columns $ :pct-ehcps [:transition-count :denominator] #(dfn// %1 %2))
      (tc/order-by $ numerator-grouping-keys))))

(defn need-summaries [config-path sim-prefix transitions-path]
  (summarise (read-simulation-data config-path sim-prefix)
             {:domain-key :need
              :historic-transitions-count (historic-transition-counts transitions-path)
              :simulation-count (get-in (ws/read-config config-path) [:projection-parameters :simulations])
              :transform-simulation-f transform-need-simulation}))

(defn transform-need-provision-simulation
  [sim {:keys [numerator-grouping-keys denominator-grouping-keys historic-transitions-count
               provision]}]
  (let [census (-> (tc/concat-copying historic-transitions-count sim)
                   (tr/transitions->census))
        denominator (-> census
                        (tc/group-by denominator-grouping-keys)
                        (tc/aggregate {:denominator #(dfn/sum (:transition-count %))}))]
    (as-> census $
      (tc/map-columns $ :provision [:setting :academic-year]
                      (fn [setting ncy] (setting->lsrp-provision setting ncy)))
      (tc/select-rows $ #(#{provision} (:provision %)))
      (tc/map-columns $ :need [:need]
                      (fn [need] (need->lsrp-need need)))
      (tc/group-by $ numerator-grouping-keys)
      (tc/aggregate $ {:transition-count #(dfn/sum (:transition-count %))})
      (tc/group-by $ :need {:result-type :as-seq})
      (map #(td/add-diff % :transition-count) $)
      (apply tc/concat $)
      (tc/rename-columns $
                         {:diff :ehcp-diff
                          :pct-diff :ehcp-pct-diff})
      (tc/inner-join $ denominator denominator-grouping-keys)
      (tc/map-columns $ :pct-ehcps [:transition-count :denominator] #(dfn// %1 %2))
      (tc/order-by $ numerator-grouping-keys))))

(defn early-years-need-summaries [config-path sim-prefix transitions-path]
  (summarise (read-simulation-data config-path sim-prefix)
             {:domain-key :need
              :provision "Early Years settings including PVIs"
              :historic-transitions-count (historic-transition-counts transitions-path)
              :simulation-count (get-in (ws/read-config config-path) [:projection-parameters :simulations])
              :transform-simulation-f transform-need-provision-simulation}))

(defn format-5-1 [summary]
  (-> summary
      (tc/select-columns [:calendar-year :age-group :median])
      (tc/select-rows #(lsrp-calendar-years (:calendar-year %)))
      (tc/pivot->wider :calendar-year :median)
      (tc/replace-missing :all :value 0.0)
      (tc/union (->empty-ds (vals lsrp-age-group-names) :age-group))
      (tc/unique-by :age-group)
      (tc/order-by [(comp (into {} (map (fn [k v] (assoc {} k v))
                                        (vals lsrp-age-group-names)
                                        (range 1 (+ 1 (count lsrp-age-group-names))))) :age-group)])
      (tc/rename-columns {:age-group "Calendar Year"})
      (tc/set-dataset-name "5.1 Total number of EHC plans by age group (with estimated future projections)")))

(defn format-7 [summary]
  (-> summary
      (tc/select-columns [:calendar-year :need :median])
      (tc/select-rows #(lsrp-calendar-years (:calendar-year %)))
      (tc/pivot->wider :calendar-year :median)
      (tc/replace-missing :all :value 0.0)
      (tc/union (->empty-ds lsrp-needs :need))
      (tc/unique-by :need)
      (tc/order-by [(comp (into {} (map (fn [k v] (assoc {} k v))
                                        lsrp-needs
                                        (range 1 (+ 1 (count lsrp-needs))))) :need)])
      (tc/rename-columns {:need "Calendar Year"})
      (tc/set-dataset-name "7. Current and projected number of all CYP with EHC plans by primary need")))

(defn format-7-1 [summary]
  (-> summary
      (tc/select-columns [:calendar-year :need :median])
      (tc/select-rows #(lsrp-calendar-years (:calendar-year %)))
      (tc/pivot->wider :calendar-year :median)
      (tc/replace-missing :all :value 0.0)
      (tc/union (->empty-ds lsrp-needs :need))
      (tc/unique-by :need)
      (tc/order-by [(comp (into {} (map (fn [k v] (assoc {} k v))
                                        lsrp-needs
                                        (range 1 (+ 1 (count lsrp-needs))))) :need)])
      (tc/rename-columns {:need "Calendar Year"})
      (tc/set-dataset-name "7.1 Current and projected number of all CYP with EHC plans in Early Years Settings including PVIs by primary need")))

;; Assumptions:
;; - Projected values are the median of 1000 simulations, as such a summing of median values will not result in the same value as the total median

;; Requirements
;; - Projection, including the prefix
;; - Historic transitions file
;; - Map of LA settings to LSRP "provision"
;; - Map of LA Primary Needs to LSRP Primary Needs
