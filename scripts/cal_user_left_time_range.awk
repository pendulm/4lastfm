BEGIN {FS="|"; OFS="|"; last = "*RoRo*"; min = 1367251200; }
{
    if ($1 == last) 
	min = min < $3 ? min : $3;
    else {
	print  last, min;
	last = $1;
	min = $3;
    }
}
END { print last, min; }    
    
