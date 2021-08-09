set term svg dynamic size 800,800
set output "output.svg"

bin_width=10000000
bin(x,width)=width*floor(x/width) + bin_width/2.0

# set xrange [0:1000]
set yrange [0:100]

set style fill solid 1.0 noborder
set boxwidth bin_width
plot [0:5][0:*] "20_peers.log" u (bin($1,bin_width)):(1.0) smooth freq with boxes
