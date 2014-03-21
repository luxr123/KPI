register rt.jar

define DecodeURl InvokeForString('java.net.URLDecoder.decode', 'String String');


A = LOAD 'url.txt' AS (url: chararray);
B = FOREACH A GENERATE DecodeURL(url, 'UTF-8');
--dump B;
STORE B INTO 'url';
