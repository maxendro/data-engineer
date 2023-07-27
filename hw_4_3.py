SELECT iris_name,
       GREATEST (atr1_max,
                 atr2_max,
                 atr3_max,
                 atr4_max)    max_attr,
       LEAST (atr1_min,
              atr2_min,
              atr3_min,
              atr4_min)       min_attr
  FROM (  SELECT MAX (i.attr1)     atr1_max,
                 MIN (i.attr1)     atr1_min,
                 MAX (i.attr2)     atr2_max,
                 MIN (i.attr2)     atr2_min,
                 MAX (i.attr3)     atr3_max,
                 MIN (i.attr3)     atr3_min,
                 MAX (i.attr4)     atr4_max,
                 MIN (i.attr4)     atr4_min,
                 i.iris_name
            FROM kkuznetsov.iris i
        GROUP BY iris_name)