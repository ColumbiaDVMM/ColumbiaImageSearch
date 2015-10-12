# Columbia University Content Based Image Search Tool
http://www.ee.columba.edu/MEMEX

This repository contains the content based image similarity search and concept prediction for the DARPA MEMEX project developped at Columbia University by Tao Chen, Svebor Karaman and Shih-Fu Chang.

PHP API files:
- The image search tool is accesed through the ColumbiaUimgSearch.php file.
Given an image URL passed as argument it will download it, and then look for similar images using the getSimilarNew.py python script.
- The concept prediction tool is accessed through the sentibank.php file. Given an image URL, it gives a score for all the Sentibank concepts using the sentibank.py python script.
