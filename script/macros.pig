define ADD(xx)
returns res {
    $res = foreach $xx generate x + y;
};

