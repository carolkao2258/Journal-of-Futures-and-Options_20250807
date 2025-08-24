/* 建議：先解除 NOEXEC，開啟訊息方便除錯 */
options notes stimer source nosyntaxcheck;

/* 匯入資料 */
proc import out=patent_regression_data5_announce
     datafile="/home/u64061874/patent_regression_data5_announce.xlsx"
     dbms=xlsx replace;
     getnames=yes;
run;

proc import out=founding_date
     datafile="/home/u64061874/founding_date.xlsx"
     dbms=xlsx replace;
     getnames=yes;
run;

/* （可選）若 founding_date 可能同一 companyID 有多列，先去重 */
proc sort data=founding_date nodupkey;
  by companyID;
run;

/* 合併：注意 fouuding_date 的實際拼法；也可順便 RENAME 成 founding_date */
proc sql;
  create table patent_regression_data6_announce as
  select e.*,
         s.fouuding_date as founding_date,   /* ← 改名成 founding_date，來源是 fouuding_date */
         s.withdraw_date
  from patent_regression_data5_announce as e
  left join founding_date as s
    on cats(e.companyID) = cats(s.companyID)
  ;
quit;

/* 排序 */
proc sort data=patent_regression_data6_announce;
  by year companyID;
run;

/* 檢查筆數（假設你已定義過 %chk 巨集） */
%chk(patent_regression_data6_announce);

/* 匯出 */
proc export data=patent_regression_data6_announce
  outfile="/home/u64061874/patent_regression_data6_announce.xlsx"
  dbms=xlsx replace;
  sheet="patent_regression_data6_announce";
  putnames=yes;
run;

/* 篩選公司ID，產出 patent_regression_data7_announce */
data patent_regression_data7_announce;
  set patent_regression_data6_announce;
  where companyID in (
    "000980","6008","6016","000960","000884","000616","6005",
    "000815","000116","000888","000930","0009A0","000885","000779",
    "2856","000025","2854"
  );
run;

/* 確認前 20 筆 */
proc print data=patent_regression_data7_announce(obs=20); 
run;

/*匯出*/
proc export data=patent_regression_data7_announce
  outfile="/home/u64061874/patent_regression_data7_announce.xlsx"
  dbms=xlsx replace;
  sheet="patent_regression_data7_announce";
  putnames=yes;
run;