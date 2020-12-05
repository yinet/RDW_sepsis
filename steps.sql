--sepsis3表记录 12529
--去除同一个病人多次住院经历，病人数为 10375

with s as
(select * from (select  ROW_NUMBER() OVER(PARTITION BY subject_id ORDER BY suspected_infection_time) row , * FROM `physionet-data.mimic_derived.sepsis3`)
where row =1  order by subject_id,suspected_infection_time
)
SELECT 
i.subject_id
,i.hadm_id
,i.stay_id
,i.intime
,s.suspected_infection_time
,s.sofa_time
,s.sofa_score
,i.first_careunit
,i.last_careunit
,i.outtime
,i.los 
FROM `physionet-data.mimic_icu.icustays` i
inner join s
on i.stay_id= s.stay_id
order by i.subject_id, i.hadm_id, i.intime, s.suspected_infection_time
--------------------------------------------------------------------

1.提取 RDW
--------------------------------------------------------------------
SELECT * FROM `physionet-data.mimic_hosp.labevents` WHERE itemid=51277
--------------------------------------------------------------------

2.合并sepsis3+icustays+patients+admissions   病人数为 10375
--------------------------------------------------------------------
with s as
(select * from (select  ROW_NUMBER() OVER(PARTITION BY subject_id ORDER BY suspected_infection_time) row , * FROM `physionet-data.mimic_derived.sepsis3`)
where row =1  order by subject_id,suspected_infection_time
)
--身高
,height as     
(SELECT * FROM `physionet-data.mimic_icu.chartevents` 
where itemid =226730  --cm
--or itemid =226707 --inch
order by hadm_id, stay_id, charttime
)
--体重
,weight as     
(SELECT 
hadm_id,stay_id,charttime,valuenum
--,round(case when valueuom='kg' then valuenum else valuenum*0.4545 end,1) as weight_kg
,valueuom
FROM `physionet-data.mimic_icu.chartevents` 
where
itemid =226512  --kg
--or itemid =226531 --lbs
order by hadm_id, stay_id, charttime
)

SELECT 
s.subject_id
, i.hadm_id
, s.stay_id
, case when p.gender='F' then 1 else 0 end as gender   -- 0-male,1-female
, p.anchor_age
, height.valuenum as heigth_cm
, weight.valuenum as weigth_kg
, CAST(COALESCE(prg.pregnant, 0) AS INT64) AS pregnant
, i.intime
, i.outtime
, s.suspected_infection_time
, s.sofa_time
, s.sofa_score  
, i.los
, p.dod
, a.admittime
, a.dischtime
, i.first_careunit
, i.last_careunit 
,a.deathtime
,a.discharge_location
,a.hospital_expire_flag
FROM  s
inner join `physionet-data.mimic_icu.icustays` i 
on s.stay_id = i.stay_id
--and i.los>=1
left join  `physionet-data.mimic_core.patients` p
on s.subject_id = p.subject_id
--and anchor_age>18
left join  `physionet-data.mimic_core.admissions` a
on i.hadm_id = a.hadm_id
LEFT JOIN (
  SELECT stay_id, MAX(valuenum) as pregnant
  FROM physionet-data.mimic_icu.chartevents
  WHERE itemid = 225082 -- Pregnant
  GROUP BY 1
) prg
  ON i.stay_id = prg.stay_id
LEFT JOIN height 
  on i.stay_id=height.stay_id
LEFT JOIN weight 
  on i.stay_id=weight.stay_id
ORDER BY s.subject_id, i.hadm_id, i.intime


--------------------------------------------------------------------

3.去除重复数据
duplicates drop  hadm_id stay_id intime outtime,force

4.部分病人有转 ICU 记录，造成同一次住院期间有两个 stay_id，如hadm_id =20041437，删除？
解决办法：
将入 ICU 时间和出 ICU 时间转换为icu_intime，icu_outtime
生成一个新变量，new_icu_outtime，同一次住院期间出现在各 ICU单元治疗转科的，最后一次的icu_outtime的值最大
sort hadm_id intime
by hadm_id: egen new_icu_outtime = max(icu_outtime)
	根据新变量重新生成new_icu_los
		gen new_icu_los=(new_icu_outtime-icu_intime)/24
5.剔除ICU 住院时间小于 1 天的患者:   -807
drop  if new_icu_los<1
6.一个 subject_id有多个hadm_id 的，根据 in_time，只保留第一住院的
duplicates drop subject_id ,force    -1754
