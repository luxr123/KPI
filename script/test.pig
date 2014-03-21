set pig.import.search.path '/home/hadoop/lu/kpi/script';
import 'macros.pig';

--%default n '1';
%default time '20140220';

define add(xx, time)
returns res {
    $res = foreach $xx generate CONCAT((chararray)(x + y), (chararray)$time);
};

A = load '$n' using PigStorage(',') as (x:int, y:int);
result = add(A, $time);

B = foreach A generate (x/y/y)%y;
--dump result;
illustrate result;

dump B;
