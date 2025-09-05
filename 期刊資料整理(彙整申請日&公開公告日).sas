/*匯入資料*/
proc import out=patent_regression_data6_announce
     datafile="/home/u64061874/patent_regression_data6_announce.xlsx"
     dbms=xlsx replace;
     getnames=yes;
run;

proc import out=patent_regression_data6_apply
     datafile="/home/u64061874/patent_regression_data6_apply.xlsx"
     dbms=xlsx replace;
     getnames=yes;
run;

/* ===== 1) announce：專利欄位加 _ann，建立連結鍵 ===== */
data _ann;
  set patent_regression_data6_announce
      (rename=(
        invention_acc = invention_acc_ann
        new_acc       = new_acc_ann
        design_acc    = design_acc_ann
        total_acc     = total_acc_ann
        invention_sin = invention_sin_ann
        new_sin       = new_sin_ann
        design_sin    = design_sin_ann
        total_sin     = total_sin_ann
      ));
  length keyID $40 keyYear $8;
  /* companyID：數值→z6.；字串→strip+upcase */
  if vtype(companyID)='N' then keyID = putn(companyID, 'z6.');
  else keyID = strip(upcase(cats(companyID)));
  keyYear = cats(year);
run;

/* ===== 2) apply：專利欄位加 _app，建立連結鍵 ===== */
data _app;
  set patent_regression_data6_apply
      (rename=(
        invention_acc = invention_acc_app
        new_acc       = new_acc_app
        design_acc    = design_acc_app
        total_acc     = total_acc_app
        invention_sin = invention_sin_app
        new_sin       = new_sin_app
        design_sin    = design_sin_app
        total_sin     = total_sin_app
      ));
  length keyID $40 keyYear $8;
  if vtype(companyID)='N' then keyID = putn(companyID, 'z6.');
  else keyID = strip(upcase(cats(companyID)));
  keyYear = cats(year);
run;

/* ===== 3) FULL JOIN 合併（year+companyID） ===== */
proc sql; reset exec; quit;  /* 若前一步報錯，先恢復 EXEC */
proc sql;
  create table prd6_raw as
  select
    /* 鍵值與基本識別欄 */
    coalesce(input(a.keyYear,8.), input(b.keyYear,8.)) as year,
    coalescec(a.keyID, b.keyID)                        as companyID_char length=40,
    coalescec(a.companyName, b.companyName)            as companyName length=256,

    /* announce 專利欄位 */
    a.invention_acc_ann, a.new_acc_ann, a.design_acc_ann, a.total_acc_ann,
    a.invention_sin_ann, a.new_sin_ann, a.design_sin_ann, a.total_sin_ann,

    /* apply 專利欄位 */
    b.invention_acc_app, b.new_acc_app, b.design_acc_app, b.total_acc_app,
    b.invention_sin_app, b.new_sin_app, b.design_sin_app, b.total_sin_app,

    /* 共同數值欄位合一 */
    coalesce(a.ROE,                   b.ROE)                   as ROE,
    coalesce(a.cost_to_income_ratio,  b.cost_to_income_ratio)  as cost_to_income_ratio,
    coalesce(a.Zscore,                b.Zscore)                as Zscore,
    coalesce(a.size,                  b.size)                  as size,
    coalesce(a.FL,                    b.FL)                    as FL,
    coalesce(a.current_ratio,         b.current_ratio)         as current_ratio,
    coalesce(a.market_share,          b.market_share)          as market_share,
    coalesce(a.ARTR,                  b.ARTR)                  as ARTR,
    coalesce(a.PQ,                    b.PQ)                    as PQ,
    coalesce(a.gGDP,                  b.gGDP)                  as gGDP,
    coalesce(a.income_structure,      b.income_structure)      as income_structure,
    coalesce(a.marginFac1,            b.marginFac1)            as marginFac1,
    coalesce(a.marginFac2,            b.marginFac2)            as marginFac2,
    coalesce(a.foreign_company,       b.foreign_company)       as foreign_company,
    coalesce(a.foreign_dummy,         b.foreign_dummy)         as foreign_dummy,
    coalesce(a.securities_age,        b.securities_age)        as securities_age,
    /* 日期/字串欄位合一 */
    coalesce(a.founding_date,         b.founding_date)         as founding_date,
    coalesce(a.withdraw_date,         b.withdraw_date)         as withdraw_date,
    coalescec(a.TSE_number,           b.TSE_number)            as TSE_number length=32,
    coalescec(a.TSE_name,             b.TSE_name)              as TSE_name   length=128,
    coalescec(a.TEJ_number,           b.TEJ_number)            as TEJ_number length=32,
    coalescec(a.TEJ_name,             b.TEJ_name)              as TEJ_name   length=128

  from _ann as a
  full join _app as b
    on a.keyID   = b.keyID
   and a.keyYear = b.keyYear
  ;
quit;

/* ===== 4) 整理 companyID / 排序 / 欄位順序 ===== */
data prd6_prep;
  set prd6_raw;
  length companyID $40;
  companyID = companyID_char;   /* 保前導0的字串型 companyID */
  drop companyID_char;
run;

proc sort data=prd6_prep out=prd6_srt;
  by year companyID;
run;

/* 依指定欄位順序輸出最終資料集 */
data patent_reg6_merged;
  retain
    year companyID companyName
    /* _ann 專利欄位 */
    invention_acc_ann new_acc_ann design_acc_ann total_acc_ann
    invention_sin_ann new_sin_ann design_sin_ann total_sin_ann
    /* _app 專利欄位 */
    invention_acc_app new_acc_app design_acc_app total_acc_app
    invention_sin_app new_sin_app design_sin_app total_sin_app
    /* 共同合一欄位 */
    ROE cost_to_income_ratio Zscore size FL current_ratio market_share ARTR PQ gGDP income_structure
    marginFac1 marginFac2 foreign_company foreign_dummy securities_age
    founding_date withdraw_date
    TSE_number TSE_name TEJ_number TEJ_name
  ;
  set prd6_srt;
run;

/* 可選：輸出 Excel */
proc export data=patent_reg6_merged
  outfile="/home/u64061874/patent_regression_data6_merged.xlsx"
  dbms=xlsx replace;
  sheet="merged6";
  putnames=yes;
run;