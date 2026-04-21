(ns witan.send.lsrp
  (:require [witan.send.adroddiad.analysis.total-domain :as td]
            [witan.send.adroddiad.transitions :as tr]
            [witan.send :as ws]
            [witan.send.lsrp.domains :as dom]
            [tablecloth.api :as tc]
            [ham-fisted.reduce :as hf-reduce]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.dataset.reductions :as ds-reduce]
            [clojure.string :as s]))

(defn read-simulation-data [config-path sim-prefix]
  (td/simulation-data-from-config config-path sim-prefix))

(defn historic-transition-counts [transitions-path]
  (td/historic-ehcp-count (tc/dataset transitions-path {:key-fn keyword})))

(defn ->empty-ds [domains key]
  (tc/dataset
   (map (fn [v] (assoc (reduce (fn [m k] (assoc m k 0.0)) {} dom/lsrp-calendar-years) key v)) domains)))

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
                      (fn [ncy] (dom/lsrp-age-group-names
                                 (dom/lsrp-age-group ncy))))
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

(defn age-group-summaries [{:keys [config-path sim-prefix transitions-path]}]
  (summarise (read-simulation-data config-path sim-prefix)
             {:domain-key :age-group
              :historic-transitions-count (historic-transition-counts transitions-path)
              :simulation-count (get-in (ws/read-config config-path) [:projection-parameters :simulations])
              :transform-simulation-f transform-age-group-simulation}))

(defn transform-provision-simulation
  [sim {:keys [numerator-grouping-keys denominator-grouping-keys historic-transitions-count]}]
  (let [census (-> (tc/concat-copying historic-transitions-count sim)
                   (tr/transitions->census))
        denominator (-> census
                        (tc/group-by denominator-grouping-keys)
                        (tc/aggregate {:denominator #(dfn/sum (:transition-count %))}))]
    (as-> census $
      (tc/map-columns $ :provision [:setting :academic-year]
                      (fn [setting ncy] (dom/setting->lsrp-provision setting ncy)))
      (tc/group-by $ numerator-grouping-keys)
      (tc/aggregate $ {:transition-count #(dfn/sum (:transition-count %))})
      (tc/group-by $ :provision {:result-type :as-seq})
      (map #(td/add-diff % :transition-count) $)
      (apply tc/concat $)
      (tc/rename-columns $
                         {:diff :ehcp-diff
                          :pct-diff :ehcp-pct-diff})
      (tc/inner-join $ denominator denominator-grouping-keys)
      (tc/map-columns $ :pct-ehcps [:transition-count :denominator] #(dfn// %1 %2))
      (tc/order-by $ numerator-grouping-keys))))

(defn provision-summaries [{:keys [config-path sim-prefix transitions-path]}]
  (summarise (read-simulation-data config-path sim-prefix)
             {:domain-key :provision
              :historic-transitions-count (historic-transition-counts transitions-path)
              :simulation-count (get-in (ws/read-config config-path) [:projection-parameters :simulations])
              :transform-simulation-f transform-provision-simulation}))

(defn transform-need-simulation
  [sim {:keys [numerator-grouping-keys denominator-grouping-keys historic-transitions-count]}]
  (let [census (-> (tc/concat-copying historic-transitions-count sim)
                   (tr/transitions->census))
        denominator (-> census
                        (tc/group-by denominator-grouping-keys)
                        (tc/aggregate {:denominator #(dfn/sum (:transition-count %))}))]
    (as-> census $
      (tc/map-columns $ :need [:need]
                      (fn [need] (dom/need->lsrp-need need)))
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

(defn need-summaries [{:keys [config-path sim-prefix transitions-path]}]
  (summarise (read-simulation-data config-path sim-prefix)
             {:domain-key :need
              :historic-transitions-count (historic-transition-counts transitions-path)
              :simulation-count (get-in (ws/read-config config-path) [:projection-parameters :simulations])
              :transform-simulation-f transform-need-simulation}))

(defn transform-need-provision-simulation
  [sim {:keys [numerator-grouping-keys denominator-grouping-keys
               historic-transitions-count provision]}]
  (let [census (-> (tc/concat-copying historic-transitions-count sim)
                   (tr/transitions->census))
        denominator (-> census
                        (tc/group-by denominator-grouping-keys)
                        (tc/aggregate {:denominator #(dfn/sum (:transition-count %))}))]
    (as-> census $
      (tc/map-columns $ :provision [:setting :academic-year]
                      (fn [setting ncy] (if (s/includes? setting "Hsp")
                                          "Hospital School"
                                          (dom/setting->lsrp-provision setting ncy))))
      (tc/select-rows $ #(provision (:provision %)))
      (tc/map-columns $ :need [:need]
                      (fn [need] (dom/need->lsrp-need need)))
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

(defn need-provision-summaries [{:keys [config-path sim-prefix transitions-path provision]}]
  (summarise (read-simulation-data config-path sim-prefix)
             {:domain-key :need
              :provision (if (set? provision)
                           provision
                           #{provision})
              :historic-transitions-count (historic-transition-counts transitions-path)
              :simulation-count (get-in (ws/read-config config-path) [:projection-parameters :simulations])
              :transform-simulation-f transform-need-provision-simulation}))

(defn early-years-need-summaries [{:keys [config-path sim-prefix transitions-path]}]
  (need-provision-summaries {:config-path config-path
                             :sim-prefix sim-prefix
                             :transitions-path transitions-path
                             :provision "Early Years settings including PVIs"}))

(defn mainstream-need-summaries [{:keys [config-path sim-prefix transitions-path]}]
  (need-provision-summaries {:config-path config-path
                             :sim-prefix sim-prefix
                             :transitions-path transitions-path
                             :provision "Mainstream schools or academies"}))


(defn format-5-1 [summary]
  (-> summary
      (tc/select-columns [:calendar-year :age-group :median])
      (tc/select-rows #(dom/lsrp-calendar-years (:calendar-year %)))
      (tc/pivot->wider :calendar-year :median)
      (tc/replace-missing :all :value 0.0)
      (tc/union (->empty-ds (vals dom/lsrp-age-group-names) :age-group))
      (tc/unique-by :age-group)
      (tc/order-by [(comp (into {} (map (fn [k v] (assoc {} k v))
                                        (vals dom/lsrp-age-group-names)
                                        (range 1 (+ 1 (count dom/lsrp-age-group-names))))) :age-group)])
      (tc/rename-columns {:age-group "Calendar Year"})
      (tc/set-dataset-name "5.1 Total number of EHC plans by age group (with estimated future projections)")))

(defn format-6 [summary]
  (-> summary
      (tc/select-columns [:calendar-year :provision :median])
      (tc/select-rows #(dom/lsrp-calendar-years (:calendar-year %)))
      (tc/pivot->wider :calendar-year :median)
      (tc/replace-missing :all :value 0.0)
      (tc/union (->empty-ds dom/lsrp-provision :provision))
      (tc/unique-by :provision)
      (tc/order-by [(comp (into {} (map (fn [k v] (assoc {} k v))
                                        dom/lsrp-provision
                                        (range 1 (+ 1 (count dom/lsrp-provision))))) :provision)])
      (tc/rename-columns {:provision "Calendar Year"})
      (tc/set-dataset-name "6. Current and projected number of all CYP with EHC plans by provision")))

(defn format-7 [summary]
  (-> summary
      (tc/select-columns [:calendar-year :need :median])
      (tc/select-rows #(dom/lsrp-calendar-years (:calendar-year %)))
      (tc/pivot->wider :calendar-year :median)
      (tc/replace-missing :all :value 0.0)
      (tc/union (->empty-ds dom/lsrp-needs :need))
      (tc/unique-by :need)
      (tc/order-by [(comp (into {} (map (fn [k v] (assoc {} k v))
                                        dom/lsrp-needs
                                        (range 1 (+ 1 (count dom/lsrp-needs))))) :need)])
      (tc/rename-columns {:need "Calendar Year"})
      (tc/set-dataset-name "7. Current and projected number of all CYP with EHC plans by primary need")))

(defn format-7-n [summary ds-name]
  (-> summary
      (tc/select-columns [:calendar-year :need :median])
      (tc/select-rows #(dom/lsrp-calendar-years (:calendar-year %)))
      (tc/pivot->wider :calendar-year :median)
      (tc/replace-missing :all :value 0.0)
      (tc/union (->empty-ds dom/lsrp-needs :need))
      (tc/unique-by :need)
      (tc/order-by [(comp (into {} (map (fn [k v] (assoc {} k v))
                                        dom/lsrp-needs
                                        (range 1 (+ 1 (count dom/lsrp-needs))))) :need)])
      (tc/rename-columns {:need "Calendar Year"})
      (tc/set-dataset-name ds-name)))

(defn format-7-1 [summary]
  (format-7-n summary "7.1 Current and projected number of all CYP with EHC plans in Early Years Settings including PVIs by primary need"))

(defn format-7-2 [summary]
  (format-7-n summary "7.2 Current and projected number of all CYP with EHC plans in Mainstream Schools or Academies (including Support Bases) by primary need"))

;; Assumptions:
;; - Projected values are the median of 1000 simulations, as such a summing of median values will not result in the same value as the total median

;; Requirements
;; - Projection, including the prefix
;; - Historic transitions file
;; - Map of LA settings to LSRP "provision"
;; - Map of LA Primary Needs to LSRP Primary Needs
;; - unique ID (often person table ID) and assessment outcome from SEN2 assessment dataset
