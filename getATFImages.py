import urllib2
import os

basepath="http://imagecat.dyndns.org/atf/images"
outbasepath="/srv/skaraman/images_ATF"
subfolder_list=["/","reddit-040815/"]
accepted_img_types=[".jpg",".png",".gif"]

for subfolder in subfolder_list:
  # Getting images list
  images_list=[]
  fileslist_html=urllib2.urlopen(basepath+subfolder)
  all_lines=fileslist_html.readlines()
  fileslist_html.close()
  for oneline in all_lines:
	print oneline
	for part in oneline.split("\""):
		for ext in accepted_img_types:
			if part.rfind(ext)!=-1 and part.rfind(ext)==len(part)-4 and part.find("/icons/")==-1:
				# We got an image
				images_list.append(part)
  
  #Saving found images
  if len(images_list)>0:
  	nb_imgs=len(images_list)
  	print "Found #"+str(),"images in subfolder ",subfolder+"."
  	print "Start saving",
  	curr_out_dir=outbasepath+subfolder
  	os.mkdir(curr_out_dir)
  	num_img=0
  	for one_image in images_list:
  		img_html = urllib2.urlopen(basepath+subfolder+one_image)
		img_data = img_html.read()
		f_img=open(curr_out_dir,"wb")
		f_img.write(img_data)
		close(f_img)
		if num_img==nb_imgs/10:
			print ".",
		num_img+=1
