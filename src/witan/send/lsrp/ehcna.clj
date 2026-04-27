(ns witan.send.lsrp.ehcna
  (:require [witan.send.lsrp.domains :as dom]
            [witan.sen2.ncy :as ncy]
            [witan.sen2 :as sen2]
            [tablecloth.api :as tc]))

(defn join-ehcna-to-person [ds person-ds outcome-key census-year]
  "expects `ds` to contain `:person-table-id` and an outcome key (e.g.
   `:assessment-outcome` or `:request-outcome`) and `person-ds` to
   contain `:person-table-id` and `:person-birth-date`"
  (-> ds
      (tc/select-columns [:person-table-id outcome-key])
      (tc/drop-rows #(#{"H"} (get % outcome-key))) ;; records of historical assessment pre-census year
      (tc/left-join person-ds :person-table-id)
      (tc/select-columns [:person-table-id outcome-key :person-birth-date])
      (tc/add-column :census-year census-year)
      (tc/left-join (sen2/census-years->census-dates-ds [census-year]) :census-year)
      (tc/rename-columns {:census-dates.census-year :census-year})
      (tc/map-columns :age-at-start-of-school-year [:census-date :person-birth-date] #(when %1 (ncy/age-at-start-of-school-year-for-date %1 %2)))
      (tc/map-columns :ncy-nominal [:age-at-start-of-school-year] ncy/age-at-start-of-school-year->ncy)))

(defn ->census [ds outcome-key]
  (-> ds
      (tc/rename-columns {:census-year     :calendar-year
                          :person-table-id :id
                          outcome-key      :setting
                          :ncy-nominal     :academic-year})
      (tc/select-columns [:id :calendar-year :setting :academic-year])
      (tc/add-column :need "NA")))

(defn ->transitions [census-ds]
  (let [transitions-n (-> census-ds
                          (tc/add-columns {:setting-1 "NONSEND"
                                           :need-1    "NONSEND"}) ;; request for assessment
                          (tc/rename-columns {:setting :setting-2
                                              :need :need-2
                                              :academic-year :academic-year-2})
                          (tc/map-columns :academic-year-1 [:academic-year-2]
                                          (fn [ncy] (dec ncy)))
                          (tc/reorder-columns [:id :calendar-year :setting-1 :need-1
                                               :academic-year-1 :setting-2 :need-2
                                               :academic-year-2]))
        transitions-n+1 (tc/map-rows transitions-n
                                     (fn [{:keys [calendar-year setting-1 setting-2
                                                  need-1 need-2 academic-year-1 academic-year-2]}]
                                       {:calendar-year (inc calendar-year)
                                        :setting-1 setting-2
                                        :need-1 need-2
                                        :academic-year-1 academic-year-2
                                        :setting-2 "NONSEND"
                                        :need-2 "NONSEND"
                                        :academic-year-2 (inc academic-year-2)}))]
    (tc/concat transitions-n transitions-n+1)))
