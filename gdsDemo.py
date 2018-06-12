from cma.micaps import GDSDataService
import time

if __name__ == "__main__":
    start = time.clock()

    # ***********************测试程序*********************************"
    with GDSDataService() as gds:
        #gds.download('D:/download', gds.get_file_list('ECMWF_HR/TMP_2M','18060220', is_absolute=True))
        gds.download('D:/download', gds.get_file_list('ECMWF_LR/PRMSL','18060320',True))
        gds.download('D:/download', gds.get_file_list('ECMWF_LR/PRMSL','18060308',True))
        #gds.download('D:/download', gds.get_file_list('ECMWF_HR/TMP_2M','18060308',True))
        print(gds.total_connect_times, gds.total_request_times)

    # ***********************测试程序*********************************"
    end = time.clock()
    elapsed = end - start
    print("Time used: %.6fs, %.6fms\n" % (elapsed, elapsed * 1000))