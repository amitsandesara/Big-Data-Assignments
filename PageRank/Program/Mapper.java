{\rtf1\ansi\ansicpg1252\cocoartf1348\cocoasubrtf170
{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;\red127\green0\blue85;\red0\green0\blue192;\red100\green100\blue100;
\red106\green62\blue62;\red42\green0\blue255;\red63\green127\blue95;}
\margl1440\margr1440\vieww10800\viewh8400\viewkind0
\deftab720
\pard\pardeftab720

\f0\b\fs18 \cf2 import
\b0 \cf0  java.io.IOException;\

\b \cf2 import
\b0 \cf0  java.util.StringTokenizer;\
\

\b \cf2 import
\b0 \cf0  org.apache.hadoop.io.LongWritable;\

\b \cf2 import
\b0 \cf0  org.apache.hadoop.io.Text;\

\b \cf2 import
\b0 \cf0  org.apache.hadoop.mapreduce.Mapper;\
\

\b \cf2 public
\b0 \cf0  
\b \cf2 class
\b0 \cf0  PageRankMapper 
\b \cf2 extends
\b0 \cf0  Mapper<LongWritable, Text, Text, Text> \{\
\

\b \cf2 private
\b0 \cf0  Text \cf3 word\cf0  = 
\b \cf2 new
\b0 \cf0  Text();\
\
\pard\pardeftab720
\cf4 @Override\cf0 \
\pard\pardeftab720

\b \cf2 public
\b0 \cf0  
\b \cf2 void
\b0 \cf0  map(LongWritable \cf5 key\cf0 , Text \cf5 value\cf0 , Context \cf5 context\cf0 )\
    
\b \cf2 throws
\b0 \cf0  IOException, InterruptedException \{\
\
	  String \cf5 line\cf0  = \cf5 value\cf0 .toString();\
	  StringTokenizer \cf5 tokenizer\cf0  = 
\b \cf2 new
\b0 \cf0  StringTokenizer(\cf5 line\cf0 , \cf6 " "\cf0 );\
	  String \cf5 startPage\cf0  = \cf5 line\cf0 .substring(0,1);\
	  String \cf5 nextString\cf0 =\cf6 ""\cf0 ;\
	  Text \cf5 startNodeT\cf0  = 
\b \cf2 new
\b0 \cf0  Text(\cf5 startPage\cf0 );\
	  \
	  \cf7 //search for 0 or . as all initial PR's < 1 \cf0 \
	  
\b \cf2 int
\b0 \cf0  \cf5 prIndex\cf0  = \cf5 line\cf0 .indexOf(\cf6 "0"\cf0 ); \
	  String \cf5 pr\cf0  = \cf5 line\cf0 .substring(\cf5 prIndex\cf0 , \cf5 line\cf0 .length());\
	  
\b \cf2 double
\b0 \cf0  \cf5 prDoubleInit\cf0  = Double.
\i parseDouble
\i0 (\cf5 pr\cf0 );\
	  \
	  \cf7 //\ul outlinks\ulnone  of each page\cf0 \
	  String \cf5 \ul \ulc5 links\cf0 \ulnone =\cf5 line\cf0 .substring(0, \cf5 prIndex\cf0 );\
	 \
	  
\b \cf2 double
\b0 \cf0  \cf5 numLinks\cf0  = (\cf5 line\cf0 .split(\cf6 " "\cf0 ).\cf3 length\cf0 -1)-1;\
	  \
	  \cf7 //PR going to each \ul outlink\cf0 \ulnone \
	  
\b \cf2 double
\b0 \cf0  \cf5 prDouble\cf0  = \cf5 prDoubleInit\cf0  / \cf5 numLinks\cf0 ;\
	  String \cf5 prString\cf0  = Double.
\i toString
\i0 (\cf5 prDouble\cf0 );\
	  Text \cf5 prText\cf0  = 
\b \cf2 new
\b0 \cf0  Text(\cf5 prString\cf0 );\
	  \
	  
\b \cf2 while
\b0 \cf0  (\cf5 tokenizer\cf0 .hasMoreTokens()) \{\
		  \cf5 nextString\cf0 = \cf3 word\cf0 .toString();\
		  
\b \cf2 if
\b0 \cf0  (!\cf5 nextString\cf0 .matches(\cf6 ""\cf0 ) && !\cf5 nextString\cf0 .matches(\cf5 startPage\cf0 ) && \cf5 nextString\cf0 .length()==1)\{\
			  Text \cf5 outlink\cf0  = 
\b \cf2 new
\b0 \cf0  Text(\cf5 nextString\cf0 );\
			  \cf5 context\cf0 .write(\cf5 outlink\cf0 , \cf5 prText\cf0 );\
			  \cf5 context\cf0 .write(\cf5 startNodeT\cf0 ,\cf5 outlink\cf0 );\
		  \}\
		  \cf3 word\cf0 .set(\cf5 tokenizer\cf0 .nextToken());\
	  \}\
\}\
\}\
}