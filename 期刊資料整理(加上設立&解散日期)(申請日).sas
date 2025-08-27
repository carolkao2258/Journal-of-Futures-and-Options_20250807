/* 建議：先開啟訊息，方便除錯；確認穩定後再關閉 */
options notes stimer source nosyntaxcheck;

/* ========== 1) 匯入資料 ========== */
proc import out=patent_regression_data5_apply
     datafile="/home/u64061874/patent_regression_data5_apply.xlsx"
     dbms=xlsx replace; getnames=yes; run;

proc import out=founding_date
     datafile="/home/u64061874/founding_date.xlsx"
     dbms=xlsx replace; getnames=yes; run;

proc import out=securities_age
     datafile="/home/u64061874/securities_age.xlsx"
     dbms=xlsx replace; getnames=yes; run;

proc import out=basic_data
     datafile="/home/u64061874/basic_data.xlsx"
     dbms=xlsx replace; getnames=yes; run;

/* ========== 2) 去重（避免同一公司多筆） ========== */
proc sort data=founding_date  nodupkey; by companyID; run;
proc sort data=securities_age nodupkey; by companyID; run;
proc sort data=basic_data     nodupkey; by companyID; run;

/* ========== 3) 合併：securities_age → founding_date → basic_data ========== */
/* 若 founding_date.xlsx 欄位其實叫 founding_date，請把 s.fouuding_date 改為 s.founding_date */
proc sql;
  create table _data6_merge_raw as
  select e.*,
         a.age_year      as securities_age,
         s.fouuding_date as founding_date,
         s.withdraw_date as withdraw_date,
         b.TSE_number,
         b.TSE_name,
         b.TEJ_number,
         b.TEJ_name
  from patent_regression_data5_apply as e
  left join securities_age as a
    on cats(e.companyID)=cats(a.companyID)
  left join founding_date as s
    on cats(e.companyID)=cats(s.companyID)
  left join basic_data as b
    on cats(e.companyID)=cats(b.companyID)
  ;
quit;

/* ========== 4) 調整欄位順序（僅保留指定欄位；不帶入 whole_name / TEJ_date_adj） ========== */
data patent_regression_data6_apply;
  retain
    year companyID companyName
    invention_acc new_acc design_acc total_acc
    invention_sin new_sin design_sin total_sin
    ROE cost_to_income_ratio Zscore size FL current_ratio market_share ARTR PQ gGDP
    income_structure marginFac1 marginFac2 foreign_company foreign_dummy
    securities_age founding_date withdraw_date
    TSE_number TSE_name TEJ_number TEJ_name
  ;
  set _data6_merge_raw
      (keep=
        year companyID companyName
        invention_acc new_acc design_acc total_acc
        invention_sin new_sin design_sin total_sin
        ROE cost_to_income_ratio Zscore size FL current_ratio market_share ARTR PQ gGDP
        income_structure marginFac1 marginFac2 foreign_company foreign_dummy
        securities_age founding_date withdraw_date
        TSE_number TSE_name TEJ_number TEJ_name
      );
run;

/* ========== 5) 排序 & 匯出 data6 (apply) ========== */
proc sort data=patent_regression_data6_apply;
  by year companyID;
run;

proc export data=patent_regression_data6_apply
  outfile="/home/u64061874/patent_regression_data6_apply.xlsx"
  dbms=xlsx replace;
  sheet="patent_regression_data6_apply";
  putnames=yes;
run;

/* ========== 6) 依公司ID篩選 → data7 (apply)（用 cats() 避免型態衝突） ========== */
data patent_regression_data7_apply;
  set patent_regression_data6_apply (where=(
    cats(companyID) in (
      "000980","6008","6016","000960","000884","000616","6005",
      "000815","000116","000888","000930","0009A0","000885","000779",
      "2856","000025","2854","000102"
    )
  ));
run;

proc export data=patent_regression_data7_apply
  outfile="/home/u64061874/patent_regression_data7_apply.xlsx"
  dbms=xlsx replace;
  sheet="patent_regression_data7_apply";
  putnames=yes;
run;