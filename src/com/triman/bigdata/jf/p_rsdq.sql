create or replace procedure P_zdry_rsdq is  --重点人员入室盗窃

--step 1  积分模型已经在扒窃拎包之前建立

--step 2 通过积分模型建立人员积分档案

   Cursor cur_user_info is select * from t_zdry_jbxx_old t  ;
             vTotal    number := 0; --总分
         vScore_age    number := 0; --年龄积分
         vScore_sex    number := 0; --性别积分
        vScore_area    number := 0; --区域积分

      vScore_dynamc    number := 0; --动态积分
         isCountNum    number := 0;
        vScore_deal    number := 0; --处理结果分
          vNum_this    number := 0; --前科积分

       vInternetBar    number := 0; --网吧积分
           vBarArea    number := 0; --两周跨越区域积分
          vNum_shbx    number := 0; --社会保险

               vAge    number := 0; --年龄

               vSql    VARCHAR2(1000); --sql 语句

         vStartTime    varchar2(14); --执行开始时间
         vEndTime      varchar2(14); --执行结束时间
         vCount        number := 0; --执行条数

         vLastTime     varchar2(20); --最后更新时间

begin

     vSql := 'select to_char(sysdate,''yyyymmddhh24miss'') from dual';
     execute immediate vSql into vStartTime;

     for user_info in cur_user_info  loop

          vTotal := 0;

          --根据出生日期，计算年龄
          vSql := 'select floor(months_between(sysdate, to_date('''||user_info.csrq||''',''yyyy-mm-dd''))/12) age from dual';
          execute immediate vSql into vAge;

          --根据年龄积分
          if vAge between 18 and 21 then
             vScore_age := 10.0;
          elsif vAge = 17 or vAge = 22 or vAge = 23 or vAge = 24 then
             vScore_age := 8.0;
          elsif vAge = 16 or vAge = 25 or vAge = 26 or vAge = 27 or vAge = 28 then
             vScore_age := 6.0;
          elsif vAge between 29 and 33 then
             vScore_age := 5.0;
          elsif vAge between 34 and 36 then
             vScore_age := 3.0;
          end if;

          --根据年龄设定系数 男性系数为1，女性系数为0.2
          if user_info.xb = 1 then
             vScore_sex := 1;
          else
             vScore_sex := 0.1;
          end if;

          --根据籍贯进行积分，建立高危地域表，可维护
          vSql := 'select count(1) from d_zdry_gwdy_rsdq t where t.dm = '''||user_info.jg||'''' ;
          execute immediate vSql into isCountNum;
          if isCountNum >0 then
             vSql := 'select t.score from d_zdry_gwdy_rsdq t where t.dm = '''||user_info.jg||'''' ;
             execute immediate vSql into vScore_area;
          else
             vScore_area := 0;
          end if;

          --计算入室盗窃前科 数据
          vSql := 'select count(*) from wsba.hx_a_ajjbqk@ywhj132 w
          where w.ajbh in (select ajbh from vw_wsba_hx_r_xyrc@ywhj132 v where v.sfzhm = '''
          ||user_info.zjhm||''') and (zabmc like ''%入室盗窃%'' ) and AJZTDM in (''204'', ''299'')';
          execute immediate vSql into vNum_this;

          -- 前科明细
          -- 把明细记录到t_zdry_detail_result 中，插入时间区分不同的记录，每条数据代表一条记录，具体sql语句在 FIELD_DETAIL字段中。
          vLastTime := to_char(sysdate,'yyyymmddhh24miss');
          if vNum_this > 0 then
            for v_criminal_record in (select guid from wsba.hx_a_ajjbqk@ywhj132 w where w.ajbh in (select ajbh from vw_wsba_hx_r_xyrc@ywhj132 v where v.sfzhm = user_info.zjhm) and (zabmc like '%入室盗窃%' ) and AJZTDM in ('204', '299')) loop
                begin
                    insert into t_zdry_detail_result ( ID , IDENTITYCARD_NO,PERSON_NAME, FIELD_SCORE, INSERT_TIME, FIELD_TYPE, FIELD_DETAIL, FIELD_VALUE ,TABLENAME)
                    values (user_info.rid,user_info.zjhm,user_info.xm, 5,vLastTime, '入室盗窃前科',
                    'select * from wsba.hx_a_ajjbqk@ywhj132 w where w.guid = '''||v_criminal_record.guid||'''',
                    v_criminal_record.guid , 'wsba.hx_a_ajjbqk@ywhj132');
                    commit;
                end;
            end loop;
          end if;

          --根据入室盗窃犯罪前科，计算处理分。 需要改进，细化到年份
          --vScore_deal := user_info.num_this * 5 + user_info.num_last * 4 + user_info.num_before * 3 + user_info.num_othre *２ + user_info.num_gambling_drug * 2;
          vScore_deal := vNum_this * 5;

          --网吧类积分
          --同日内在不同网吧零点到五点上网次数大于等于三次
          vSql :=  'select count(1) from (select count(substr(login_at, 0, 8)) from syrk.t_wb_trace@ywhj132 t where substr(login_at,9,2) in (''00'',''01'',''02'',''03'',''04'',''05'') and t.id_code = '''||user_info.zjhm||''' having count(substr(login_at, 0, 8)) >= 3 group by substr(login_at, 0, 8))';
          execute immediate vSql into vInternetBar;
          if vInternetBar > 0 then
             --vTotal := vScore_sex * (vScore_age + vScore_area + vScore_deal ) * ;

             vScore_dynamc := 3;
          else
             vScore_dynamc := 0;
          end if;

          --两周内跨越三个以上区活动的
          vSql := 'select count(1) from (select area_code, substr(login_at, 0, 8), count(1)  from syrk.t_wb_trace@ywhj132 t where floor(sysdate - to_date(substr(t.login_at,0,8),''yyyy-mm-dd'')) >14 and  t.id_code = '''||user_info.zjhm||''' group by area_code, substr(login_at, 0, 8) )';
          execute immediate vSql into vBarArea;
          if vBarArea > 0 then
             vScore_dynamc := vScore_dynamc + 2;
          end if;

          --上述两种都出现
          if vInternetBar> 0 and vBarArea> 0 then
             vScore_dynamc := vScore_dynamc + 10;
          end if;

          --社会保险
          vSql := 'select count(1) from syrk.t_rb_shbx@syrk19 where zjhm = '''||user_info.zjhm||'''';
          execute immediate vSql into vNum_shbx;
          if vNum_shbx > 0 then
             vScore_dynamc :=  vScore_dynamc - 50;
          end if;

          --计算Total
          vTotal := vScore_sex * (vScore_age + vScore_area + vScore_deal ) * vScore_dynamc;

          --update t_zdry_jbxx t set score4 = vTotal, SCORE_OF_QK4 = vNum_this ,t.zhgxsj_rsdq = vLastTime where t.id = user_info.id;

          insert into t_zdry_score_result (ZJHM,SCORE4,SCORE_OF_QK4,ZHGXSJ_RSDQ)
          values（user_info.zjhm,vTotal,vNum_this, vLastTime);

          commit;

     end loop;

     vSql := 'select to_char(sysdate,''yyyymmddhh24miss'') from dual';
     execute immediate vSql into vEndTime;

     vSql := 'select count(1) from t_zdry_jbxx_old t '  ;
     execute immediate vSql into vCount;

     insert into T_ZDRY_LOG (STARTTIME, ENDTIME, TOTALTIME, TABLENAME,RECORDCOUNT, DETAILACTION)
            values(vStartTime, vEndTime, round(to_number(to_date(vEndTime,'yyyymmddhh24miss') - to_date(vStartTime,'yyyymmddhh24miss'))*24*60*60), 'PRIAPWEB.t_zdry_jbxx_old'，vCount, '入室盗窃数据');
     commit;

end P_zdry_rsdq;
