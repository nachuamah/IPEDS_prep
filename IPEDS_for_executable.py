# Import relevant libraries
from urllib.request import urlopen
from tempfile import NamedTemporaryFile
from shutil import unpack_archive
import re
import io
import subprocess
import time
import sys
import os

while True:
    while True:
        while True: 
            #Establish which year you are interested in (year at end of schoolyear)
            exact_time = time.localtime()
            current_year = exact_time[0]
            try:
                year_input = int(input('Enter the year at end of schoolyear of interest: '))
                if year_input < 2002:
                    print('Please enter a year after 2001.')
                    continue
                if year_input > current_year:
                    print('Please enter an earlier year.')
                    continue
                else:
                    print('Loading...')
                    break
            except ValueError:
                print('Please enter the year as an integer.')
                continue

        # Generate variables for later use
        completions_dta_year = year_input - 1
        year = str(year_input)
        completions_dta_year = str(completions_dta_year)
        comp_dta_name_initial = 'dct_s' + completions_dta_year + '_is'
        comp_dta_name_final = 'US_' + completions_dta_year
        inst_dta_name_initial = 'dct_hd' + year
        inst_dta_name_final = 'Dict_' + year
        final_completions_dta = 'US_' + year
        confirm_merge = completions_dta_year + '_merge_confirmation.log'

        #URLS for downloads
        completions_dofile_url = 'https://nces.ed.gov/ipeds/datacenter/data/C' + year + '_A_Stata.zip'
        completions_csvfile_url = 'https://nces.ed.gov/ipeds/datacenter/data/C' + year + '_A_Data_Stata.zip'
        completions_dictionary_url = 'https://nces.ed.gov/ipeds/datacenter/data/C' + year + '_A_Dict.zip'
        institution_do_url = 'https://nces.ed.gov/ipeds/datacenter/data/HD' + year + '_Stata.zip'
        institution_csv_url = 'https://nces.ed.gov/ipeds/datacenter/data/HD' + year + '_Data_Stata.zip'
        institution_dictionary_url = 'https://nces.ed.gov/ipeds/datacenter/data/HD' + year + '_Dict.zip'

        # Generate filepaths as strings for later use
        home_directory = os.getcwd()
        completions_dofile_path = home_directory + '/c' + year + '_a_v2.do'
        comp_logfile_path = home_directory + '/c' + year + '_a.log'
        inst_logfile_path = home_directory + '/hd' + year + '.log'
        stata_app_path = '/Applications/Stata/StataMP.app/Contents/MacOS/stata-mp'
        comp_dta_path_final = '\"' + home_directory + '/' + comp_dta_name_final + '\"'
        merge_file_path = home_directory + 'merge_file'
        inst_dta_path_final = '\"' + home_directory + '/' + inst_dta_name_final + '\"'
        final_completions_dta_path = '\"' + home_directory + '/' + final_completions_dta + '\"'
        confirm_merge_path = home_directory + '/' + 'merge'


        # Define function to generate path of downloaded and unzipped file
        def url_to_docpath(source_url, working_directory, str_file_type):
            firststep = source_url.find(str(year))
            target = firststep
            while source_url[target] != '/':
            	target = target - 1
            dot = source_url.find('.', target)
            file_name_upper = source_url[target + 1:dot + 1]
            file_name_lower = file_name_upper.lower()
            file_path = working_directory + '/' + file_name_lower + str_file_type
            return(file_path)

        # Download unzip and save .csv with institution data
        try:
            zipurl = institution_csv_url
            with urlopen(zipurl) as zipresp, NamedTemporaryFile() as tfile:
                tfile.write(zipresp.read())
                tfile.seek(0)
                unpack_archive(tfile.name, home_directory, format = 'zip')
            break
        except:
            print('This year\'s data is not available. Please enter an earlier year.')
            continue

    institution_csvfile_path = url_to_docpath(zipurl, home_directory, 'csv')


    # Download unzip and save .xlsx with dictionary for institutions data
    zipurl = institution_dictionary_url
    with urlopen(zipurl) as zipresp, NamedTemporaryFile() as tfile:
        tfile.write(zipresp.read())
        tfile.seek(0)
        unpack_archive(tfile.name, home_directory, format = 'zip')
    institution_dictionary_path = url_to_docpath(zipurl, home_directory, 'csv')

    # Download unzip ans save .do with instructions for creation of institutions dataset
    zipurl = institution_do_url
    with urlopen(zipurl) as zipresp, NamedTemporaryFile() as tfile:
        tfile.write(zipresp.read())
        tfile.seek(0)
        unpack_archive(tfile.name, home_directory, format = 'zip')
    institution_do_path = url_to_docpath(zipurl, home_directory, 'do')
    setup_inst_dofile_path_initial = institution_do_path.replace('_stata.do','.do')
    setup_inst_dofile_path_final = institution_do_path.replace('_stata.do','_v2.do')

    # Edit institutions .do file to create institutions .dta file
    with open(setup_inst_dofile_path_initial, 'r', encoding = 'ascii', errors = 'ignore') as input_file, open(setup_inst_dofile_path_final, 'w') as output_file:
        for line in input_file:
            if line.find('insheet using') != -1:
                if line.find(':\\') != -1:
                    path_start = line.find(':\\')
                    path_end = line.find('\"', path_start)
                    line = line[:path_start - 1] + institution_csvfile_path + line[path_end:]
                    output_file.write(line)
            elif line.find('label data') != -1:
                if line.find('\"') != -1:
                    path_start = line.find('\"')
                    path_end = line.find('\"', path_start + 1)
                    line = line[:path_start] + '\"' + inst_dta_name_final + line[path_end:]
                    output_file.write(line)
            elif line.find(' save ') != -1:
                remove_line = '\n'
                output_file.write(remove_line)
            else:
                output_file.write(line)
        output_file.write('\n save ' + inst_dta_path_final + '\n')

    # Running institution .do through the command line
    cmd1a = 'cd ' + home_directory
    cmd1b =  stata_app_path + ' < ' + setup_inst_dofile_path_final + ' > ' + inst_logfile_path + ' &'
    subprocess.call(cmd1a, shell=True)
    subprocess.call(cmd1b, shell=True)


    # Download unzip and save .csv with completions data
    zipurl = completions_csvfile_url
    with urlopen(zipurl) as zipresp, NamedTemporaryFile() as tfile:
        tfile.write(zipresp.read())
        tfile.seek(0)
        unpack_archive(tfile.name, home_directory, format = 'zip')
    completions_csvfile_path = url_to_docpath(zipurl, home_directory, 'csv')

    # Download unzip and save .xlsx with dictionary for completions data
    zipurl = completions_dictionary_url
    with urlopen(zipurl) as zipresp, NamedTemporaryFile() as tfile:
        tfile.write(zipresp.read())
        tfile.seek(0)
        unpack_archive(tfile.name, home_directory, format = 'zip')
    completions_dictionary_path = url_to_docpath(zipurl, home_directory, 'csv')

    # Download unzip and save .do with instructions for creation of completions dataset
    zipurl = completions_dofile_url
    with urlopen(zipurl) as zipresp, NamedTemporaryFile() as tfile:
        tfile.write(zipresp.read())
        tfile.seek(0)
        unpack_archive(tfile.name, home_directory, format = 'zip')
    completions_do_path = url_to_docpath(zipurl, home_directory, 'do')
    setup_comp_dofile_path_initial = completions_do_path.replace('_stata.do','.do')
    setup_comp_dofile_path_final = completions_do_path.replace('_stata.do','_v2.do')


    # Edit and run completions .do file to create completions .dta file
    with open(setup_comp_dofile_path_initial, 'r', encoding = 'ascii', errors = 'ignore') as input_file, open(setup_comp_dofile_path_final, 'w') as output_file:
        for line in input_file:
            if line.find('insheet using') != -1:
                if line.find(':\\') != -1:
                    path_start = line.find(':\\')
                    path_end = line.find('\"', path_start)
                    line = line[:path_start - 1] + completions_csvfile_path + line[path_end:]
                    output_file.write(line)
            elif line.find('label data') != -1:
                if line.find('\"') != -1:
                    path_start = line.find('\"')
                    path_end = line.find('\"', path_start + 1)
                    line = line[:path_start] + '\"' + comp_dta_name_final + line[path_end:]
                    #line = line.replace(comp_dta_name_initial, com[_dta_name_final)
                    output_file.write(line)
            elif line.find(' save ') != -1:
                remove_line = '\n'
                output_file.write(remove_line)
            else:
                output_file.write(line)
        output_file.write('\n save ' + comp_dta_path_final + '\n')
            
    input_file.close()
    output_file.close()


    # Running completions .do through the command line
    cmd2a = 'cd ' + home_directory
    cmd2b =  stata_app_path + ' < ' + completions_dofile_path + ' > ' + comp_logfile_path + ' &'
    subprocess.call(cmd2a, shell=True)
    subprocess.call(cmd2b, shell=True)

    # Create and execute .do file to merge institution characteristics with completions data
    with open(merge_file_path, 'w') as merge_do:
        merge_do.write('use ' + comp_dta_path_final)
        merge_do.write('\n merge m:1 unitid using ' + inst_dta_path_final)
        merge_do.write('\n drop if _merge == 2 \n drop _merge \n') 
        merge_do.write('\n save ' + final_completions_dta_path +'\n')
    merge_do.close

    time.sleep(15)

    # Running recently created merge .do through the command line
    cmd3a = 'cd ' + home_directory
    cmd3b =  stata_app_path + ' < ' + merge_file_path + ' > ' + confirm_merge_path + ' &'
    subprocess.call(cmd3a, shell=True)
    subprocess.call(cmd3b, shell=True)

    # Confirm completion
    print('Task Complete.')
    
    # Giving the option to restart program, closes while loop
    try:
        done = input('Would you like to download another year? (y/n): ')
        if done in ('y', 'n'):
            if done == 'y':
                continue
            else:
                break
        else:
            print('Invalid input. Either enter \'y\' for \'yes\' or \'n\' for \'no\'.')
            continue
    except:
        print('Let\'s try this again.')
        continue

sys.exit('Okay, bye!')

