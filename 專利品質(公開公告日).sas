/* 建議：先開啟訊息，除錯完再關 */
options notes stimer source syntaxcheck;

/* ========== 0) 清掉本次 announce 臨時表，保留 apply ========== */
proc datasets lib=work nolist;
  delete 
    patent_data_raw_announce
    patent2_announce
    max_lag_announce
    patent_lag_index_announce
    patent_PQ_announce
    patent_PQ_clean_announce
    avg_PQ_announce_inter
    avg_PQ_announce_final
    merged_announce
    avg_PQ_announce2
    avg_PQ_announce_sorted
  ;
quit;

/* ========== 1) 匯入資料（announce 版） ========== */
proc import out=patent_data_raw_announce
    datafile="/home/u64061874/patent_data_announce.xlsx"
    dbms=xlsx replace;
    getnames=yes;
run;

/* 匯入公司對照表 */
proc import out=number4
     datafile="/home/u64061874/number4.xlsx"
     dbms=xlsx replace;
     getnames=yes;
run;

/* ========== 2) 日期轉換（用公告日取 announce_year） ========== */
data patent2_announce;
    set patent_data_raw_announce;

    apply_date  = inputn(strip(申請日), 'yymmdd10.');
    grant_date  = inputn(strip(公開公告日), 'yymmdd10.');

    if apply_date = . or grant_date = . then grant_lag = .;
    else grant_lag = grant_date - apply_date;

    announce_year = year(grant_date);   /* ★ 改用公告日取年份 */
    patent_type   = 專利類型;

    length 申請人_raw $200;
    申請人_raw = strip(申請人);

    format apply_date grant_date yymmdd10.;
run;

/* ========== 3) 公告年度 × 專利類型 的最大 grant_lag ========== */
proc sql;
    create table max_lag_announce as
    select announce_year, patent_type, max(grant_lag) as max_grant_lag
    from patent2_announce
    group by announce_year, patent_type;
quit;

/* ========== 4) 計算 cohort 標準化指數 ========== */
proc sql;
    create table patent_lag_index_announce as
    select a.*,
           b.max_grant_lag,
           case 
             when a.grant_lag >= 0 and b.max_grant_lag > 0 
               then 1 - (a.grant_lag / b.max_grant_lag)
             else .
           end as grant_lag_index
    from patent2_announce as a
    left join max_lag_announce as b
      on a.announce_year = b.announce_year
     and a.patent_type   = b.patent_type;
quit;

/* ========== 5) PQ 指標 ========== */
data patent_PQ_announce;
    set patent_lag_index_announce;
    PQ = grant_lag_index;
    drop grant_lag_index;
run;

/* ========== 6) 公司名清理 ========== */
data patent_PQ_clean_announce;
    set patent_PQ_announce;
    length 公司名_clean $80;
    retain re;
    if _n_ = 1 then
        re = prxparse('/(.*?股份有限公司|.*?有限責任公司|.*?有限公司|.*?公司)/');

    if not missing(申請人_raw) and prxmatch(re, 申請人_raw) then 
        公司名_clean = prxposn(re, 1, 申請人_raw);
    else 
        公司名_clean = 申請人_raw;
run;

/* ========== 7) 每年 × 公司 平均 PQ （公告年） ========== */
proc means data=patent_PQ_clean_announce noprint;
    class announce_year 公司名_clean;
    var PQ;
    output out=avg_PQ_announce_inter mean=avg_PQ;
run;

/* 去掉總計 */
data avg_PQ_announce_final;
    set avg_PQ_announce_inter;
    if not missing(announce_year) and not missing(公司名_clean);
    keep announce_year 公司名_clean avg_PQ;
run;

/* ========== 8) 連結 id，整理欄位 ========== */
proc sql;
    create table merged_announce as
    select a.*, b.id
    from avg_PQ_announce_final as a
    left join number4 as b
      on a.公司名_clean = b.name;
quit;

data avg_PQ_announce2;
    set merged_announce;
    if not missing(id);
    length company $80;
    company = 公司名_clean;
    retain announce_year company id avg_PQ;
    keep announce_year company id avg_PQ;
run;

/* ========== 9) 輸出最終表：avg_PQ_announce_sorted ========== */
proc sort data=avg_PQ_announce2 out=avg_PQ_announce_sorted;
    by announce_year id;
run;

proc export data=avg_PQ_announce_sorted
     outfile="/home/u64061874/avg_PQ_announce_sorted.xlsx"
     dbms=xlsx replace;
run;

/* 檢查前 20 筆 */
proc print data=avg_PQ_announce_sorted (obs=20) label noobs;
    label announce_year="announce_year"
          company      ="company"
          id           ="id"
          avg_PQ       ="avg_PQ";
run;