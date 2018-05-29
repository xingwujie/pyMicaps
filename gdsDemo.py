from cma.micaps import GDSDataService
import time

if __name__ == "__main__":
    start = time.clock()

    # ***********************测试程序*********************************"
    with GDSDataService() as gds:
        #print(list(gds.get_file_list('ECMWF_HR/TMP_2M', '18052708')))
        print(gds.get_data('ECMWF_HR/TMP_2M', '18052708.000'))
    #for i in gds.get_file_'ECMWF_HR/TMP_2M', '18052708')list('GERMAN_HR/APCP','18030620'):
    #    print(i)
    #print(list(gds.get_file_list('GERMAN_HR/APCP/18030620.000')))
    #for i in gds.walk('GERMAN_HR'):
    #    print(i)
    #print(gds._recur_nums,gds._request_nums)
    # squential_test()
    #print(gds.is_directory('ECMWF_ENSEMBLE_PRODUCT/GRID_PROBABILITY/RAIN24/60.0MM/18030320.234'))
    #print(gds.get_latest_data_name('ECMWF_HR/TMP/850','*.240'))
    # gds.download_data_sequential('D:/ECMWF','ECMWF_HR/TMP/850','18012308')
    # gds.bulk_download('D:/ECMWF','ECMWF_HR/TMP/850','171217')
    # gds.download_data('D:/ECMWF', 'ECMWF_HR/TMP/850', '171218')

    # write_to_xml(gds, 'ECMWF_HR', 'D:/micapsdata.xml')
    # gds.bulk_download('ECMWF_HR','D:/ECMWF')
    # for i in gds.get_directory('ECMWF_HR','backup','12','micapsdata.xml'):
    #     print(i)

    # ***********************测试程序*********************************"
    end = time.clock()
    elapsed = end - start
    print("Time used: %.6fs, %.6fms\n" % (elapsed, elapsed * 1000))