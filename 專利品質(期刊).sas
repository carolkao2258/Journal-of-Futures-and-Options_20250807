/* 匯入資料 */
proc import out=patent_data
    datafile= "/home/u64061874/patent_data.xlsx"
    dbms=xlsx replace;
    getnames=yes;
run;

/* 1. 日期字串轉換，並以申請日年度和專利類型分組 */
data patent2;
    set patent_data;
    apply_date = input(申請日, yymmdd10.);
    grant_date = input(公開公告日, yymmdd10.);
    if apply_date = . or grant_date = . then grant_lag = .;
    else grant_lag = grant_date - apply_date;
    apply_year = year(apply_date);
    patent_type = 專利類型;   /* 直接引用你的新欄位 */
    format apply_date grant_date yymmdd10.;
run;

/* 2. 以申請年度+專利類型分群，取 cohort 內最大 grant_lag */
proc sql;
    create table max_lag as
    select apply_year, patent_type, max(grant_lag) as max_grant_lag
    from patent2
    group by apply_year, patent_type;
quit;

/* 3. 合併主表，計算 cohort 標準化 Grant Lag 指數 */
proc sql;
    create table patent_lag_index as
    select a.*, 
           b.max_grant_lag,
           case 
                when a.grant_lag >= 0 and b.max_grant_lag > 0 
                    then 1 - (a.grant_lag / b.max_grant_lag)
                else .
           end as grant_lag_index
    from patent2 as a
    left join max_lag as b
      on a.apply_year = b.apply_year
     and a.patent_type = b.patent_type;
quit;

/* 4. 檢視前20筆結果 */
proc print data=patent_lag_index(obs=20); 
run;

/*把patent_lag_index改成改成PQ*/
data patent_PQ;
    set patent_lag_index;
    PQ = grant_lag_index;
    drop grant_lag_index;
run;

data patent_PQ_clean;
    set patent_PQ;
    drop AG AH AI AJ AK AL AM AN AO AP;
run;

/* 匯出正確版本 */
proc export data=patent_PQ_clean
    outfile="/home/u64061874/patent_PQ.xlsx"
    dbms=xlsx
    replace;
run;

/* 2. 新增公司名_clean欄位：分離公司名稱 */
data patent_PQ_clean;
    set patent_PQ;
    length 公司名_clean $80;
    retain re;
    if _n_ = 1 then
        re = prxparse('/(.*?股份有限公司|.*?有限責任公司|.*?有限公司|.*?公司)/');
    if prxmatch(re, 申請人1) then 公司名_clean = prxposn(re, 1, 申請人1);
    else 公司名_clean = 申請人1;
run;

/* 3. 列印前40筆結果，確認公司名稱分類正確性 */
proc print data=patent_PQ_clean(obs=40);
    var 申請人1 公司名_clean;
run;

/* 4. 分析每年每公司平均專利品質 */
proc means data=patent_PQ_clean noprint;
    class apply_year 公司名_clean;
    var PQ;
    output out=avg_PQ mean=avg_PQ;
run;

/* 5. 僅保留有效分群（去除總計） */
data avg_PQ_final;
    set avg_PQ;
    if missing(apply_year)=0 and missing(公司名_clean)=0;
    keep apply_year 公司名_clean avg_PQ;
run;

/* 6. 輸出統計結果，確認分析正確性 */
proc print data=avg_PQ_final label noobs;
    label apply_year="年分"
          公司名_clean="公司名"
          avg_PQ="平均專利品質";
run;

*匯出資料;
proc export data=avg_PQ_final
     outfile='/home/u64061874/avg_PQ_final.xlsx'
     dbms=xlsx replace;
run;

/*匯入資料*/
proc import out=number4
     datafile="/home/u64061874/number4.xlsx"
     dbms=xlsx replace;
     getnames=yes;
run;

proc sql;
    create table merged_final as
    select a.*, b.id
    from avg_PQ_final as a
    left join number4 as b
    on a.公司名_clean = b.name;
quit;

/* 僅保留有 id 的紀錄，並排列正確欄位順序 */
data avg_PQ_final2;
    set merged_final;
    if not missing(id); /* 僅保留有 id 的 */
    retain apply_year company id avg_PQ;
    company = 公司名_clean;
    keep apply_year company id avg_PQ;
run;

proc print data=avg_PQ_final2 label noobs;
    label apply_year="apply_year"
          company="company"
          id="id"
          avg_PQ="avg_PQ";
run;

proc sort data=avg_PQ_final2 out=avg_PQ_final2_sorted;
    by apply_year id;
run;

/* 檢查排序後前20筆 */
proc print data=avg_PQ_final2_sorted(obs=20) label noobs;
    label apply_year="apply_year"
          company="company"
          id="id"
          avg_PQ="avg_PQ";
run;

/*匯出資料*/ 
proc export data=avg_PQ_final2_sorted
     outfile='/home/u64061874/avg_PQ_final2_sorted.xlsx'
     dbms=xlsx replace;
run;