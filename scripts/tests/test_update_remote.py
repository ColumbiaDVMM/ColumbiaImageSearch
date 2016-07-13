import sys
sys.path.append('../..')
import numpy as np
import base64
from cu_image_search.indexer.hbase_indexer import HBaseIndexer
from cu_image_search.memex_tools.sha1_tools import get_SHA1_from_file
from cu_image_search.memex_tools.binary_file import read_binary_file

if __name__=="__main__":
    HBI = HBaseIndexer('../../conf/global_var_remotehbase.json')
    #batch = [(u'704FCD3C4D673DE631BF985CFEABCB121A4EB7CF1FD135E1C98CFA02268FA035', u'https://s3.amazonaws.com/memex-images/full/bdb9cdde29d5a29411fe7a7dc1ca501a37303431.jpg', {u'_type': u'escorts', u'_timestamp': 1464761131138, u'_index': u'memex-domains_2016.01', u'_score': 0.0, u'fields': {u'obj_parent': [u'2E7904456709725B2B1F3108683D6895FCE0BDF8BFA29E34FD799FF8AAE4CFE5'], u'timestamp': [u'2016-01-26T12:31:11'], u'obj_original_url': [u'http://469-978-3183.escortphonelist.com/images/escort-phone-list.jpg'], u'obj_stored_url': [u'https://s3.amazonaws.com/memex-images/full/bdb9cdde29d5a29411fe7a7dc1ca501a37303431.jpg']}, u'_id': u'704FCD3C4D673DE631BF985CFEABCB121A4EB7CF1FD135E1C98CFA02268FA035'}), (u'3F98B492B2BA98676202271F85D2E3EF0D1E02B9A1C706E76D53C8AAA374494B', u'https://s3.amazonaws.com/memex-images/full/7f667480e65f17e2bec4c3da58bd407d06c4276a.jpg', {u'_type': u'escorts', u'_timestamp': 1464761131138, u'_index': u'memex-domains_2016.01', u'_score': 0.0, u'fields': {u'obj_parent': [u'44D3364D05EF7544A4B8ED36738FAFC9F56651D29D76C930DB73588A5221434B'], u'timestamp': [u'2016-01-26T12:32:20'], u'obj_original_url': [u'http://images.eroticmugshots.com/cities/63/large/21490623-2.jpg'], u'obj_stored_url': [u'https://s3.amazonaws.com/memex-images/full/7f667480e65f17e2bec4c3da58bd407d06c4276a.jpg']}, u'_id': u'3F98B492B2BA98676202271F85D2E3EF0D1E02B9A1C706E76D53C8AAA374494B'}), (u'701DA3E5D25EB3329D710C7181ED6DA9D60D081711822EDA056D29F808E39A95', u'https://s3.amazonaws.com/memex-images/full/219e1c7ba41154f5dada28d1a0bddac280900f0d.jpg', {u'_type': u'escorts', u'_timestamp': 1464761131138, u'_index': u'memex-domains_2016.01', u'_score': 0.0, u'fields': {u'obj_parent': [u'7FE5C4C07A16FFFC22E359BD067D15C816F04BD433950C95767CE81DDB441923'], u'timestamp': [u'2016-01-26T12:32:11'], u'obj_original_url': [u'http://images.eroticmugshots.com/cities/13/thumbnail/34348493-1.jpg'], u'obj_stored_url': [u'https://s3.amazonaws.com/memex-images/full/219e1c7ba41154f5dada28d1a0bddac280900f0d.jpg']}, u'_id': u'701DA3E5D25EB3329D710C7181ED6DA9D60D081711822EDA056D29F808E39A95'}), (u'6D7387E477A206C965EF1535BB83E004147531CE99F9AA14D2B3FD6FC94B22F1', u'https://s3.amazonaws.com/memex-images/full/173446f78bf85d8baf94456c4918e3941e72ee80.jpg', {u'_type': u'escorts', u'_timestamp': 1464761131138, u'_index': u'memex-domains_2016.01', u'_score': 0.0, u'fields': {u'obj_parent': [u'1E61CBB6E97B170EF81926D38E0CFBCD4F31763C1461051447713CF4601E8E7D'], u'timestamp': [u'2016-01-26T12:32:16'], u'obj_original_url': [u'http://images.escortsincollege.com/cities/126/large/10593977-3.jpg'], u'obj_stored_url': [u'https://s3.amazonaws.com/memex-images/full/173446f78bf85d8baf94456c4918e3941e72ee80.jpg']}, u'_id': u'6D7387E477A206C965EF1535BB83E004147531CE99F9AA14D2B3FD6FC94B22F1'}), (u'5B369C1A8E426BC5A5A44FDCAC9D9CE378094967AFEAC184A49A5BAF22E62EF5', u'https://s3.amazonaws.com/roxyimages/5c76b19d1548351973868f9ff97d9c0a52f918cb.jpg', {u'_type': u'escorts', u'_timestamp': 1464761131889, u'_index': u'memex-domains_2016.01', u'_score': 0.0, u'fields': {u'obj_parent': [u'51FEA8D56B4AECCDEFB4469A08DDECD29413334D6575F43B6268CA026E85E586'], u'timestamp': [u'2015-12-17T23:06:11'], u'obj_original_url': [u'http://images4.backpage.com/imager/u/large/125355586/3ef7f93df039c1fa1b6dadfa6c482745.jpg'], u'obj_stored_url': [u'https://s3.amazonaws.com/roxyimages/5c76b19d1548351973868f9ff97d9c0a52f918cb.jpg']}, u'_id': u'5B369C1A8E426BC5A5A44FDCAC9D9CE378094967AFEAC184A49A5BAF22E62EF5'}), (u'0A775A8BDE19A6EA640636F02FD451056D5FDA2300844DCC0AFBC652C02BA37B', u'https://s3.amazonaws.com/roxyimages/3f844e64033e87ca67cfde1154bec0578ee6e2cb.jpg', {u'_type': u'escorts', u'_timestamp': 1464761131889, u'_index': u'memex-domains_2016.01', u'_score': 0.0, u'fields': {u'obj_parent': [u'BD7FB7967E393CEFA88185132FC3106C5DF162F6F63A21AAD6632170F9EDC858'], u'timestamp': [u'2015-12-30T05:58:09'], u'obj_original_url': [u'http://images2.backpage.com/imager/u/large/230328867/01bf2acb8bdf85b6df93aeeb2394c11b.jpg'], u'obj_stored_url': [u'https://s3.amazonaws.com/roxyimages/3f844e64033e87ca67cfde1154bec0578ee6e2cb.jpg']}, u'_id': u'0A775A8BDE19A6EA640636F02FD451056D5FDA2300844DCC0AFBC652C02BA37B'}), (u'CAFB17AB66382E6834B1AB6ED854750054188789532F80DE99B7253B811BDB1F', u'https://s3.amazonaws.com/roxyimages/54f226755c7371c1651c5b0920ca563b89e03860.jpg', {u'_type': u'escorts', u'_timestamp': 1464761131889, u'_index': u'memex-domains_2016.01', u'_score': 0.0, u'fields': {u'obj_parent': [u'1383B4D668C03367999D645EC4418A450AC5FF5D19EE0681E39CCB4E7FA00642'], u'timestamp': [u'2015-12-16T23:58:17'], u'obj_original_url': [u'http://images4.backpage.com/imager/u/large/125277948/f58eca086858632543b39fcd0d2636fd.jpg'], u'obj_stored_url': [u'https://s3.amazonaws.com/roxyimages/54f226755c7371c1651c5b0920ca563b89e03860.jpg']}, u'_id': u'CAFB17AB66382E6834B1AB6ED854750054188789532F80DE99B7253B811BDB1F'}), (u'C0BABA15173709DA7A56207670578312CA30A7E94EDBF68A8140D8D5AA3C20FC', u'https://s3.amazonaws.com/memex-images/full/2d7348b6561c9329f8f8841dc929738ada17e95c.jpg', {u'_type': u'escorts', u'_timestamp': 1464761131973, u'_index': u'memex-domains_2016.01', u'_score': 0.0, u'fields': {u'obj_parent': [u'60C23254F4EADC5B927316AF6596BFC81CBE4187AE00419F7EAB20424F34ED91'], u'timestamp': [u'2016-01-26T13:15:21'], u'obj_original_url': [u'http://img13.asexyservice.com/4/p/200/56a6f1147f975rfRkwq_jZw8Dfd2a72kQGmO6PRwRaWKjXWdp4_200.jpg'], u'obj_stored_url': [u'https://s3.amazonaws.com/memex-images/full/2d7348b6561c9329f8f8841dc929738ada17e95c.jpg']}, u'_id': u'C0BABA15173709DA7A56207670578312CA30A7E94EDBF68A8140D8D5AA3C20FC'}), (u'6687736C0D7E5702DFBA1B94EC8F75E17CE956C5649C4354F5CDD828AEB226D6', u'https://s3.amazonaws.com/memex-images/full/229d071eafac66b81b47c7e7da041c6c91fc9ecf.jpg', {u'_type': u'escorts', u'_timestamp': 1464761131973, u'_index': u'memex-domains_2016.01', u'_score': 0.0, u'fields': {u'obj_parent': [u'5B193E4B99B3C64CA403D0EF4231D8E58554360103DBF069E09C9308B12FEFFC'], u'timestamp': [u'2016-01-26T13:15:18'], u'obj_original_url': [u'http://img7.asexyservice.com/Z/A/200/56a6f114be85a8267wNaP8K32cfsGA_emZr_DrkTMg1mZC84AZ_200.jpg'], u'obj_stored_url': [u'https://s3.amazonaws.com/memex-images/full/229d071eafac66b81b47c7e7da041c6c91fc9ecf.jpg']}, u'_id': u'6687736C0D7E5702DFBA1B94EC8F75E17CE956C5649C4354F5CDD828AEB226D6'}), (u'70520066B7DB04EF9B01858AFC652E973A2098B6039DBDB014063696B0480B82', u'https://s3.amazonaws.com/roxyimages/9bb2b24ccc451467da2e7ae89782b2d9ede6b0c0.jpg', {u'_type': u'escorts', u'_timestamp': 1464761132012, u'_index': u'memex-domains_2016.01', u'_score': 0.0, u'fields': {u'obj_parent': [u'CADCDF5FE92191882A3786FC2A80D3C55F4429F8C35DAA2BB68EC0554EDEEBB2'], u'timestamp': [u'2015-11-17T06:36:19'], u'obj_original_url': [u'http://images2.backpage.com/imager/u/large/222290798/8fd6fdf9abf273e7b235105742f54403.jpg'], u'obj_stored_url': [u'https://s3.amazonaws.com/roxyimages/9bb2b24ccc451467da2e7ae89782b2d9ede6b0c0.jpg']}, u'_id': u'70520066B7DB04EF9B01858AFC652E973A2098B6039DBDB014063696B0480B82'})]
    #readable_images = LI.index_batch(batch)
    #print readable_images
    table_cdrids_infos = 'escorts_images_cdrid_infos_dev' 
    cdrids_to_check = ['2C14463FD757BBA032446AF047B9756770F122A43FBB5DC8D35BD1E4235F51E2','3398E24ED8CC66142102F6A3F8BD62C3845B9BC2BFE18A38FFC79E970F104F98','D036974B29DE9D71A1436C870ED5FDC399417E605031EFC3D92AEB799FD1AAB0']
    s3urls_to_check = ['https://s3.amazonaws.com/roxyimages/4247b7ba85fd1fc1338d13857bfb1f2150051431.jpg', 'https://s3.amazonaws.com/memex-images/full/b5d5adc85694050f2c89cca77edcc11b1ee23035.jpg', 'https://s3.amazonaws.com/memex-images/full/ed5398d619a92afa77be1061439c4bcbfd3a6182.jpg']

    table_sha1_infos = 'escorts_images_sha1_infos_ext_dev'
    #sha1_to_check =['66816617992E776800011936B815A0A0A955190D','007C9627D0A22E9EFE5B1343850264DEA6B2440C','0014CCAF145012270059A8A0CCE95C67C29499A3']
    sha1_to_check = ['007C9627D0A22E9EFE5B1343850264DEA6B2440C', '0014CCAF145012270059A8A0CCE95C67C29499A3', '0182FEFCA7153EE50F6EE4A63F587A27D508CBE4']

    # precomputed
    res,ok_ids = HBI.get_precomp_from_sha1(sha1_to_check,["sentibank","hashcode"])
    print len(ok_ids)
    print len(res)
    print len(res[0])
    print len(res[1])
    # recomputing
    update_id = 'test_hbase_indexer'
    batch = []
    for i in range(len(cdrids_to_check)):
        batch.append((cdrids_to_check[i],s3urls_to_check[i],None))
    readable_images = HBI.image_downloader.download_images(batch,update_id)
    #print readable_images
    # Compute sha1
    sha1_images = [img+(get_SHA1_from_file(img[-1]),) for img in readable_images]
    # print sha1_images
    # check sha1s
    sha1_check = True
    for i in range(len(cdrids_to_check)):
        if sha1_images[i][-1]!=sha1_to_check[i]:
            sha1_check = False
            print "Sha1 are different! {} - {}".format(sha1_images[i][-1],sha1_to_check[i])
    # check features and hashcodes
    new_files = [img[-1] for img in readable_images]
    list_feats_id = [x[0] for x in batch]
    # Compute features
    features_filename,ins_num = HBI.feature_extractor.compute_features(new_files,update_id)
    feats,feats_OK = read_binary_file(features_filename,"feats",list_feats_id,4096*4,np.float32)
    # Compute hashcodes
    hashbits_filepath = HBI.hasher.compute_hashcodes(features_filename,ins_num,update_id)
    hashcodes,hashcodes_OK = read_binary_file(hashbits_filepath,"hashcodes",list_feats_id,256/8,np.uint8)
    for i in range(len(cdrids_to_check)):
        print np.sum(np.abs(feats[i]/np.linalg.norm(feats[i])-np.frombuffer(base64.b64decode(res[0][i]),dtype=np.float32)))
        # check hashcodes too
        print np.sum(np.abs(hashcodes[i]-np.frombuffer(base64.b64decode(res[1][i]),dtype=np.uint8)))
