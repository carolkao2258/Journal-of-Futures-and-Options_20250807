/* 建議：先開啟訊息，除錯完再關 */
options notes stimer source syntaxcheck;

/* ========== 0) 清掉同名舊表，避免干擾 ========== */
proc datasets lib=work nolist;
  delete 
    patent_data_raw
    patent2_apply
    max_lag_apply
    patent_lag_index_apply
    patent_PQ_apply
    patent_PQ_clean_apply
    avg_PQ_apply_inter
    avg_PQ_apply_final
    merged_apply
    avg_PQ_apply2
    avg_PQ_apply_sorted
  ;
quit;

/* ========== 1) 匯入資料（你的來源是 patent_data_apply.xlsx） ========== */
proc import out=patent_data_raw
    datafile="/home/u64061874/patent_data_apply.xlsx"
    dbms=xlsx replace;
    getnames=yes;
run;

/* 也先匯入公司對照（name, id） */
proc import out=number4
     datafile="/home/u64061874/number4.xlsx"
     dbms=xlsx replace;
     getnames=yes;
run;

/* ========== 2) 日期轉換（只用「申請人」） ========== */
data patent2_apply;
    set patent_data_raw;

    /* 日期欄位轉 SAS 日期（原本用 yymmdd10.，沿用） */
    apply_date = inputn(strip(申請日), 'yymmdd10.');
    grant_date = inputn(strip(公開公告日), 'yymmdd10.');

    if apply_date = . or grant_date = . then grant_lag = .;
    else grant_lag = grant_date - apply_date;

    apply_year  = year(apply_date);
    patent_type = 專利類型;

    /* 只用「申請人」 */
    length 申請人_raw $200;
    申請人_raw = strip(申請人);

    format apply_date grant_date yymmdd10.;
run;

/* ========== 3) 以 申請年度 × 專利類型 找 cohort 內最大 grant_lag ========== */
proc sql;
    create table max_lag_apply as
    select apply_year, patent_type, max(grant_lag) as max_grant_lag
    from patent2_apply
    group by apply_year, patent_type;
quit;

/* ========== 4) 合併並計算 Grant Lag 指數 ========== */
proc sql;
    create table patent_lag_index_apply as
    select a.*,
           b.max_grant_lag,
           case 
             when a.grant_lag >= 0 and b.max_grant_lag > 0 
               then 1 - (a.grant_lag / b.max_grant_lag)
             else .
           end as grant_lag_index
    from patent2_apply as a
    left join max_lag_apply as b
      on a.apply_year = b.apply_year
     and a.patent_type = b.patent_type;
quit;

/* ========== 5) PQ 指標 ========== */
data patent_PQ_apply;
    set patent_lag_index_apply;
    PQ = grant_lag_index;
    drop grant_lag_index;
run;

/* ========== 6) 用「申請人」清理公司名（已移除 AG~AP 的 DROP） ========== */
data patent_PQ_clean_apply;
    set patent_PQ_apply;
    length 公司名_clean $80;
    retain re;
    if _n_ = 1 then
        re = prxparse('/(.*?股份有限公司|.*?有限責任公司|.*?有限公司|.*?公司)/');

    if not missing(申請人_raw) and prxmatch(re, 申請人_raw) then 
        公司名_clean = prxposn(re, 1, 申請人_raw);
    else 
        公司名_clean = 申請人_raw;
run;

/* ========== 7) 每年 × 公司 平均 PQ ========== */
proc means data=patent_PQ_clean_apply noprint;
    class apply_year 公司名_clean;
    var PQ;
    output out=avg_PQ_apply_inter mean=avg_PQ;
run;

/* 只保留有效分群 */
data avg_PQ_apply_final;
    set avg_PQ_apply_inter;
    if not missing(apply_year) and not missing(公司名_clean);
    keep apply_year 公司名_clean avg_PQ;
run;

/* ========== 9) 左連結 id，整理欄位 ========== */
proc sql;
    create table merged_apply as
    select a.*, b.id
    from avg_PQ_apply_final as a
    left join number4 as b
      on a.公司名_clean = b.name;
quit;

data avg_PQ_apply2;
    set merged_apply;
    if not missing(id);                 /* 僅保留有 id 的 */
    length company $80;
    company = 公司名_clean;
    retain apply_year company id avg_PQ;
    keep apply_year company id avg_PQ;
run;

/* ========== 10) 排序並輸出最終表：avg_PQ_apply_sorted ========== */
proc sort data=avg_PQ_apply2 out=avg_PQ_apply_sorted;
    by apply_year id;
run;

proc export data=avg_PQ_apply_sorted
     outfile="/home/u64061874/avg_PQ_apply_sorted.xlsx"
     dbms=xlsx replace;
run;

/* （檢查用） */
proc print data=avg_PQ_apply_sorted (obs=20) label noobs;
    label apply_year="apply_year"
          company   ="company"
          id        ="id"
          avg_PQ    ="avg_PQ";
run;