list_symptoms = c('fatigue','abdominal_pain','chest_pain','sore_throat','shortness_of_breath',
                 'skipped_meals','loss_of_smell','unusual_muscle_pains','headache','hoarse_voice','delirium','diarrhoea',
                 'fever','persistent_cough','dizzy_light_headed','eye_soreness','red_welts_on_face_or_lips','blisters_on_feet')

library(devtools)
load_all('/home/jd21/codes/exetera')
#np = reticulate::import('numpy')

session = Session()
source = session$open_dataset('abc.h5', 'r+', 'source')  # contains covid symptom data
output = session$open_dataset('playground.hdf5', 'w', 'output')
src_test = source['tests']  # dataframe
list_testid = fld.data(src_test['patient_id'])
list_testcreate = fld.data(src_test['created_at'])
out_test = output$create_dataframe('tests')

for (k in src_test$keys()){
  out_test[k] = src_test[k]
}
specdate_filter = fld.data(out_test['date_taken_specific']) != 0
date_spec = fld.data(out_test['date_taken_specific'])
date_start = fld.data(out_test['date_taken_between_start'])
date_end = fld.data(out_test['date_taken_between_end'])
#date_fin = np$where(specdate_filter == TRUE, date_spec, date_start + 0.5 * (date_end - date_start))
date_fin = ifelse(specdate_filter == TRUE, date_spec, date_start + 0.5 * (date_end - date_start))
date_effective_test = out_test$create_timestamp('date_effective_test')
date_effective_test$data$write(date_fin)

#80
results_raw = fld.data(out_test['result'])
#results_filt = np$where(np$logical_or(results_raw == 4, results_raw == 3), TRUE, FALSE)
results_filt = ifelse(results_raw %in% c(3,4), TRUE, FALSE)
for (k in out_test$keys()){
  out_test[k]$apply_filter(results_filt, in_place = TRUE)
}

sanity_filter = (date_fin == 0)
print(sum(sanity_filter))

#96
reader_mec = out_test['mechanism']
print(c(fld.length(reader_mec), fld.length(out_test['patient_id'])))
reader_ftmec = out_test['mechanism_freetext']
pcr_standard_answers = out_test$create_numeric('pcr_standard_answers', 'bool')
pcr_strong_inferred = out_test$create_numeric('pcr_strong_inferred', 'bool')
pcr_weak_inferred = out_test$create_numeric('pcr_weak_inferred', 'bool')
antibody_standard_answers = out_test$create_numeric('antibody_standard_answers', 'bool')
antibody_strong_inferred = out_test$create_numeric('antibody_strong_inferred', 'bool')
antibody_weak_inferred = out_test$create_numeric('antibody_weak_inferred', 'bool')

exeteracovid$algorithms$test_type_from_mechanism$test_type_from_mechanism_v1(session, reader_mec, reader_ftmec, pcr_standard_answers, pcr_strong_inferred, pcr_weak_inferred, antibody_standard_answers, antibody_strong_inferred, antibody_weak_inferred)


# 129
pcr_standard = fld.data(pcr_standard_answers) + fld.data(pcr_strong_inferred) + fld.data(pcr_weak_inferred)
pcr_standard = ifelse(pcr_standard>0, 1, 0)
out_test$create_numeric('pcr_standard', 'bool')$data$write(pcr_standard)

# 139
out_test_fin = output$create_dataframe('tests_fin')
writers_dict = c()
for (k in c('patient_id', 'date_effective_test', 'result', 'pcr_standard', 'converted_test')){
 if(k == 'converted_test'){
   values  <-  rep.int(0, fld.length(out_test_fin['patient_id']))
   writers_dict[k]  <-  out_test_fin$create_numeric(k, 'bool')
 }else{

   values = fld.data(out_test[k])
   if (k == 'result'){
     values = values - 3
   }
   writers_dict[k] = out_test[k]$create_like(out_test_fin, k)
   print(c(len(values),k))
 }
 writers_dict[k]$data$write(values)
}

# 159
src_asmt = source['assessments']
print(src_asmt$keys())

tcp_flat = ifelse( fld.data(src_asmt['tested_covid_positive']) < 1, 0, 1)
spans = src_asmt['patient_id']$get_spans()
firstnz_tcp_ind = session$apply_spans_index_of_max(spans, tcp_flat)
first_hct_ind = spans[0:-1]
filt_tl = first_hct_ind != firstnz_tcp_ind
sel_max_ind = session$apply_filter(filt_tl, firstnz_tcp_ind)

max_tcp_ind = session$apply_spans_index_of_max(spans, src_asmt['tested_covid_positive'])
sel_max_tcp = session$apply_indices(filt_tl, max_tcp_ind)
sel_maxtcp_ind = session$apply_filter(filt_tl, max_tcp_ind)

# 181
if(!is.na(match('usable_asmt_tests', df.keys(output)))){
  usable_asmt_tests <- output$create_dataframe('usable_asmt_tests')
}else{
  usable_asmt_tests <- output['usable_asmt_tests']
}
for ( k in c('id', 'patient_id', 'created_at', 'had_covid_test')){
  field = src_asmt[k]$create_like(usable_asmt_tests, k)
  src_asmt[k]$apply_index(sel_max_ind, target=field)
}

#191
eff_result_time = src_asmt['created_at']$create_like(usable_asmt_tests, 'eff_result_time')
src_asmt['created_at']$apply_index(sel_maxtcp_ind, target=eff_result_time)

eff_result = src_asmt['tested_covid_positive']$create_like(usable_asmt_tests, 'eff_result')
src_asmt['tested_covid_positive']$apply_index(sel_maxtcp_ind, target=eff_result)

for (k in c('tested_covid_positive')){
  tested_covid_positive = src_asmt[k]$create_like( usable_asmt_tests, k)
  src_asmt[k]$apply_index(sel_max_tcp, target=tested_covid_positive)
}

# 212
filt_deftest = fld.data(usable_asmt_tests['tested_covid_positive']) > 1
for (k in c('id', 'patient_id', 'created_at', 'had_covid_test', 'tested_covid_positive', 'eff_result_time', 'eff_result')){
  field = usable_asmt_tests[k]$create_like(usable_asmt_tests, k)
  usable_asmt_tests[k]$apply_filter(filt_deftest, target=field)
}

#222
reader_hct = fld.data(usable_asmt_tests['created_at'])
reader_tcp = fld.data(usable_asmt_tests['eff_result_time'])

delta_time = reader_tcp - reader_hct
delta_days = delta_time / 86400
print(delta_days[0:10], delta_time[0:10])
delta_days_test = usable_asmt_tests$create_numeric('delta_days_test', 'float32')
delta_days_test$data$write(delta_days)

#233
date_final_test = ifelse(delta_days < 7, reader_hct, reader_tcp - 2 * 86400)
date_final_test = usable_asmt_tests$create_timesteamp('date_final_test')
date_final_test$data$write(date_final_test)

pcr_standard = rep.int(1, fld.length(usable_asmt_tests['patient_id']))
usable_asmt_tests$create_numeric('pcr_standard', 'bool')$data$write(pcr_standard)

# 243
list_init = c('patient_id', 'date_final_test', 'tested_covid_positive', 'pcr_standard')
list_final = c('patient_id', 'date_effective_test', 'result', 'pcr_standard')
for (k in seq(len(list_init))){
  i = list_init[k]
  f = list_final[k]
  values = fld.data(usable_asmt_tests[i])
  if (f == 'result'){
    values = values - 2
  }
  print(c(len(values),f))
  writers_dict[f]$data$write(values)
}
writers_dict['converted_test']$data$write(rep.int(1, fld.length(usable_asmt_tests['patient_id'])))
converted_fin = out_test_fin['converted_test']
result_fin = fld.data(out_test_fin['result'])
pat_id_fin = fld.data(out_test_fin['patient_id'])
filt_pos = result_fin==1

#261
out_pos = output$create_dataframe('out_pos')
for (k in df.keys(out_test_fin)){
  field = out_test_fin[k]$create_like(out_pos, k)
  print(c(k, len(reader), len(filt_pos)))
  out_test_fin[k].apply_filter(filt_pos, target=field)
}
pat_pos = out_pos['patient_id']$data

#269
df.write_csv(out_pos , 'TestedPositiveTestDetails.csv')

#276
print(pat_pos[0:10])
pat_id_all = fld.data(src_asmt['patient_id'])

test2pat = exetera$core$persistence$foreign_key_is_in_primary_key(fld.data(out_pos['patient_id']),
                                              foreign_key=fld.data(src_asmt['patient_id']))

for (f in c('created_at','patient_id','treatment','other_symptoms','created_at_day','country_code','location', list_symptoms)){
  src_asmt[f]$create_like(out_pos, f)
  src_asmt[f]$apply_filter(test2pat, target=out_pos[f])
}

print( length(unique(fld.data(out_pos['patient_id'])) ))
temp = uniq_c(fld.data(out_pos['other_symptoms']))

df_other = data.frame(temp[1], temp[2])
colnames(df_other) = c('other', 'counts')
write.csv2(df_other, 'OtherSymptoms.csv')

#295
for (k in list_symptoms){
  if(!is.na(match(k, df.keys(out_pos)))){ # exists
    out_pos[k]$data$clear()
  }else{
    src_asmt[k]$create_like(out_pos, k)
  }
  src_asmt[k]$apply_filter(test2pat, target=out_pos[k])
}
sum_symp = rep.int(0, fld.length(out_pos['patient_id']))
for (k in list_symptoms){
  values = fld.data(out_pos[k])
  if( k =='fatigue' || k=='shortness_of_breath'){
    #values = np$where(values>2,np$ones_like(values),np$zeros_like(values))
    values = ifelse(values>2, 1, 0)
  }else{
    #values = np$where(values>1,np$ones_like(values),np$zeros_like(values))
    values = ifelse(values>1, 1, 0)
  }
  sum_symp = sum_symp + values
}
out_pos$create_numeric('sum_symp', 'int')$data$write(sum_symp)

#312
#symp_flat = np$where(fld.data(out_pos['sum_symp']) < 1, 0, 1)
symp_flat = ifelse(fld.data(out_pos['sum_symp']) < 1, 0, 1)
spans = out_pos['patient_id']$get_spans()
firstnz_symp_ind = session$apply_spans_index_of_max(spans, symp_flat)  # the index of max symp flat number for each patient
max_symp_check = symp_flat[firstnz_symp_ind]  # specific symp flat number
first_symp_ind = spans[0:-1]  # index of each patient
filt_asymptomatic = max_symp_check == 0  # patient symp flat no == 0, so no unhealthy asmt
filt_firsthh_symp = first_symp_ind != firstnz_symp_ind  # fist asmt  unhealthy
print(c('Number asymptomatic is ', length(spans) - 1 - sum(max_symp_check), sum(filt_asymptomatic)))
print(c('Number not healthy first is ', length(spans) - 1 - sum(filt_firsthh_symp)))
print(c('Number definitie positive is', length(spans) - 1))
spans_valid = first_symp_ind$apply_filter(filt_firsthh_symp)
pat_sel =  out_pos['patient_id']$apply_index(spans_valid)
filt_sel = exetera$core$persistence$foreign_key_is_in_primary_key(pat_sel, fld.data(out_pos['patient_id']))
spans_asymp = first_symp_ind$apply_filter(filt_asymptomatic)
pat_asymp = out_pos['patient_id']$apply_index(spans_asymp)
filt_pata = exetera$core$persistence$foreign_key_is_in_primary_key(pat_asymp, fld.data(out_pos['patient_id']))

#334
out_pos_hs = output$create_dataframe('out_pos_hs')
for ( k in c(list_symptoms, 'created_at','created_at_day','patient_id','sum_symp','country_code','location','treatment')){
 out_pos[k]$create_like(out_pos_hs, k )
 out_pos[k]$apply_filter(filt_sel, target=out_pos_hs[k])
}

df.write_csv(out_pos_hs, 'PositiveSympStartHealthyAllSymptoms.csv')

#347
out_pos_as = output$create_dataframe('out_pos_asymp')
for (k in c(list_symptoms, 'created_at', 'created_at_day', 'patient_id', 'sum_symp', 'country_code', 'location',
                          'treatment')){
  out_pos[k]$create_like( out_pos_as, k)
  out_pos[k]$apply_filter(filt_pata, target=out_pos_as[k])
}

df.write_csv(out_pos_as, 'PositiveAsympAllSymptoms')


#362
src_pat = source['patients']
filt_pat = exetera$core$persistence$foreign_key_is_in_primary_key(fld.data(out_pos_hs['patient_id']), fld.data(src_pat['id']))
list_interest = c('has_cancer','has_diabetes','has_lung_disease','has_heart_disease','has_kidney_disease','has_asthma',
                 'race_is_other', 'race_is_prefer_not_to_say', 'race_is_uk_asian', 'race_is_uk_black',
                 'race_is_uk_chinese', 'race_is_uk_middle_eastern', 'race_is_uk_mixed_other',
                 'race_is_uk_mixed_white_black', 'race_is_uk_white', 'race_is_us_asian', 'race_is_us_black',
                 'race_is_us_hawaiian_pacific', 'race_is_us_indian_native', 'race_is_us_white', 'race_other',
                 'year_of_birth','is_smoker','smoker_status','bmi_clean','is_in_uk_twins','healthcare_professional','gender','id','blood_group')
out_pat = output$create_dataframe('patient_pos')
for(k in list_interest) {
  src_pat[k]$create_like(out_pat,k)
  src_pat[k]$apply_filter(filt_pat, target=out_pat[k])
}
df.write_csv(out_pat, 'PositiveSympStartHealthy_PatDetails.csv')

#384
spans_asymp = first_symp_ind$apply_filter(filt_asymptomatic)
pat_asymp = out_pos['patient_id']$apply_index(spans_asymp)
filt_asymp = exetera$core$persistence$foreign_key_is_in_primary_key(pat_asymp, fld.data(src_pat['id']))
out_pat_asymp = output.create_dataframe('patient_asymp')
for (k in list_interest){
  src_pat[k]$create_like(out_pat_asymp, k)
  src_pat[k]$apply_filter(filt_asymp, target=out_pat_asymp[k])
}
df.write_csv(out_pat_asymp, 'PositiveAsymp_PatDetails.csv')

session$close()
