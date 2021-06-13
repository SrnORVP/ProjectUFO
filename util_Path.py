import os, sys, re
from pprint import pprint as pp
import builtins


class Style:
    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    DARKCYAN = '\033[36m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLACK = '\033[30m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    BLD = '\033[1m'
    ITALIC = '\033[3m'
    ITL = '\033[3m'
    STRIKETHROUGH = '\033[9m'
    SKT = '\033[9m'
    UNDERLINE = '\033[4m'
    UDL = '\033[4m'
    UDLBLD = '\033[1m\033[4m'
    BOLD_UNDERLINE = '\033[21m'
    BUDL = '\033[21m'
    CAUTION = '\033[91m\033[1m\033[4m'
    END = '\033[0m'
    ENDD = '\033[0m\n'
    BACKGROUND_WHITE = '\033[107m'
    BGD_WHT = BACKGROUND_WHITE
    BACKGROUND_BLACK = '\033[40m'
    BGD_BLK = '\033[40m'
    BACKGROUND_GREY = '\033[47m'
    BGD_GRY = '\033[47m'

    def inter_code(self):
        string = 'abcdefghijk'
        print(string)
        for a in range(1, 129):
            print(a, '\033[' + str(a) + 'm' + string + self.END)


def get_file_list(target_path: list, target_extension: str, target_file_hint: str = None) -> list:
    temp = []
    files = os.listdir(os.path.join(*target_path))
    # TODO sort file list
    # sorted(lstFiles, key=os.path.getmtime)
    #TODO Add hint for picking files
    for file in files:
        isFound = False
        name, ext = os.path.splitext(file)
        if target_file_hint:
            isFound = re.search(target_file_hint, name)
        else:
            isFound = True
        if ext == target_extension and '~' not in name and isFound:
            temp.append(os.path.join(*target_path, file))
    if target_file_hint:
        print(f'Files with ID "{target_file_hint}" and "{target_extension}": {len(temp)}.\n')
    else:
        print(f'Files with "{target_extension}": {len(temp)}.\n')
    return temp


def recursive_abspath(target_path):
    absolute_paths = []
    for e in os.scandir(target_path):
        if os.path.isdir(e):
            # if exclude_dir not in os.path.abspath(e):
            absolute_paths += recursive_abspath(e)
        else:
            absolute_paths.append(os.path.abspath(e))
    return absolute_paths


def Check_Excel_Exist(arrTargetPath, strTargetFileName):
    intCount = 0
    lstTemp = []
    # lstFiles = recursive_abspath(os.path.join(*arrTargetPath))
    lstFiles = os.listdir(os.path.join(*arrTargetPath))
    for strFile in lstFiles:
        strName, strExt = os.path.splitext(strFile)
        if strTargetFileName in strName and (strExt == '.xlsx' or strExt == '.xlsm') and '~' not in strName:
            strTemp = os.path.join(*arrTargetPath, strFile)
            lstTemp.append(strFile)
            intCount += 1
    if intCount > 1:
        input(f'Error: More than one "{strTargetFileName}" found.\n{lstTemp}\n\nPress Any Key to Exit')
        sys.exit()
    elif intCount == 0:
        raise FileNotFoundError(f'"{strTargetFileName}"')
        # input(f'Error: No "{strTargetFileName}" found.\n\nPress Any Key to Exit')
        strTemp = 0
        sys.exit()
    else:
        print(f'File found: "{get_relative_path(strTemp)}"')
    return strTemp


def Get_Specific_File_Name(file_target, ext_target, path_target=['.'], compulsory=True, file_hint=None, verbose=True):
    """Return filename (string) of one specific file based on 'Generic Name' and its extension.
    The script exit when multiple file or no file found, if the file is flagged as compulsory.
    Additional hint can be provided if there maybe multi files with filename specified."""
    intCounts = 0
    lstCounts = []
    strPathError = os.path.join(*path_target, file_target)
    for strFile in os.listdir(os.path.join(*path_target, )):
        strName, strExt = os.path.splitext(strFile)
        if file_target in strName and strExt == ext_target and '~' not in strName:
            if file_hint and file_hint in strName:
                intCounts += 1
                strTemp = os.path.join(*path_target, strFile)
                lstCounts.append(strTemp)
            elif not file_hint:
                intCounts += 1
                strTemp = os.path.join(*path_target, strFile)
                lstCounts.append(strTemp)
    if intCounts == 1:
        strPathOP = strTemp
        if verbose:
            print(f'File found: "{get_relative_path(strPathOP)}"')
        return strPathOP
    elif compulsory:
        if intCounts == 0:
            raise FileNotFoundError(f'"{file_target}" with "{ext_target}" in "{path_target}" of "{file_hint}"')
            # print(Style.CAUTION + 'Error' + Style.END + ': File not found "{strPathError}" with "{strTargetExt}".')
        elif intCounts > 1:
            print(f'Error: Multiple "{file_hint}"&"{file_target}" with ext "{ext_target}" found or is open:')
            pp(lstCounts, width=200)
        _ = input('Press Any Key to Exit.')
        sys.exit()
    else:
        print(f'No specific file "{file_hint}"&"{strPathError}" with ext "{ext_target}" found, operation passed.')
        return None


def print_Status(strScript, strPath):
    print(f'{strScript} is being ran on {strPath}.\n')


def get_file_IO(io_state: int, io_folder_list: list, io_file_list: list):
    import util_Date as UD
    file_input = io_file_list[io_state]
    path_input = []
    try:
        file_main = file_input['main']
        file_supplement = file_input['supp']
        print(f'Input File[0] ID: "{file_main}".')
        path_input.append(Check_Excel_Exist(io_folder_list, file_main))
        print(f' Input File[1] ID: "{file_supplement}".')
        path_input.append(Check_Excel_Exist(io_folder_list, file_supplement))
    except TypeError:
        print(f'Input File ID: "{file_input}".')
        path_input.append(Check_Excel_Exist(io_folder_list, file_input))

    file_output = io_file_list[io_state + 1]
    try:
        file_output = file_output['main']
    except TypeError:
        pass
    finally:
        print(f'Output File ID: "{file_output}".')
        path_output = os.path.join(*io_folder_list, f'{file_output}-{UD.Get_Detailed_Time()}.xlsx')
        print(path_output)

    return path_input, path_output


def get_output_name(path: str, output: str, output_ext: str, prefix: str = '') -> str:
    import util_Date as UD
    if prefix:
        prefix += '-'
    return os.path.join(path, f'{prefix}{output}-{UD.Get_Detailed_Time()}{output_ext}')


def delete_similar_outputs(path: str, output: str, output_ext: str, confirm_string: str = 'Ask'):
    print(Style.UDLBLD + 'Searching similar output file(s)' + Style.END + ':')
    delete_list = get_file_list([path], output_ext, output)
    if delete_list:
        if confirm_string:
            print(Style.UDLBLD + 'Listing similar file(s)' + Style.END + ':')
            pp(delete_list, width=200)
            confirm_string = input(f'Remove the above similar file(s)? Y/N\n')
        else:
            confirm_string = 'Y'
        if confirm_string in ['Y', 'y', 'Yes', 'yes']:
            for e in delete_list:
                os.remove(e)
            print(Style.CAUTION + 'Similar file(s) are DELETED' + Style.ENDD)
        else:
            print(Style.CAUTION + 'Similar file(s) are NOT deleted' + Style.ENDD)


def check_file_path_exist(path: str, create: bool = True):
    path = os.path.join(*path)
    if not os.path.exists(path):
        os.mkdir(path)
        print(f'Dir not found, now created: {path}')


def get_script_IO(IO_Workflows: dict, IO_Paths: dict, IO_Params: dict,
                  script_name: str, script_state: int, prefix='', **kwargs) -> tuple or str:
    pass
    # file_iput = IO_Workflows[script_name][script_state]
    # file_oput = IO_Workflows[script_name][script_state + 1]
    #
    # main, main_ext = os.path.splitext(file_iput['main'])
    # print(f'Input File[0] ID: "{main}" with extension "{main_ext}".')
    # main = Get_Specific_File_Name(main, main_ext, IO_Paths[main], isRequired=True)
    # print()
    #
    # supp = None
    # if file_iput['supp']:
    #     supp, supp_ext = os.path.splitext(file_iput['supp'])
    #     print(f'Input File[1] ID: "{supp}" with extension "{supp_ext}".')
    #     supp = Get_Specific_File_Name(supp, supp_ext, IO_Paths[supp], isRequired=False)
    #     print()
    #
    # path_iput = [main, supp]
    #
    # oput, oput_ext = os.path.splitext(file_oput['main'])
    # oput_path=IO_Paths[oput]
    # print(f'Output File ID: "{oput}" with extension "{oput_ext}".')
    # oput_join = os.path.join(*oput_path)
    # path_oput = get_output_name(oput_join, oput, oput_ext, prefix)
    # print(f'File "{path_oput}" will be created.\n')
    #
    # delete_similar_outputs(oput_join, oput, oput_ext)
    #
    # script_param = IO_Params[script_name]
    # print(f'Project Parameters for "{script_name}" at state "{script_state}" loaded:')
    # pp(script_param, compact=True)
    #
    # return path_iput, path_oput, script_param


def get_dict_IO(IO_Workflows: dict, IO_Paths: dict, IO_Params: dict,
                      script_name: str, script_state: int, prefix='', **kwargs) -> dict or str:

    file_iput = IO_Workflows[script_name][script_state]
    file_oput = IO_Workflows[script_name][script_state + 1]

    path_iput = {}
    for s, (k, v) in enumerate(file_iput.items()):
        name, ext = os.path.splitext(v)
        print(Style.UDLBLD + f'INPUT file ["{k}"]' + Style.END + f': "{name}" with extension "{ext}".')
        path_iput[k] = Get_Specific_File_Name(name, ext, IO_Paths[name], compulsory=True)
        print()

    path_oput = {}
    for s, (k, v) in enumerate(file_oput.items()):
        name, ext = os.path.splitext(v)
        path = IO_Paths[name]
        print(Style.UDLBLD + f'OUTPUT file ["{k}"]' + Style.END + f': "{name}" with extension "{ext}".')
        check_file_path_exist(path)
        oput_join = os.path.join(*path)
        path_oput[k] = get_output_name(oput_join, name, ext, prefix)
        print(f'File created: "{get_relative_path(path_oput[k])}"\n')
        if not builtins.GLOBAL_VERBOSE:
            print(Style.ITL + f'Builtin Verbose: {builtins.GLOBAL_VERBOSE}' + Style.END)
        delete_similar_outputs(oput_join, name, ext, builtins.GLOBAL_VERBOSE)

    script_param = IO_Params[script_name]
    print(Style.UDLBLD + 'Project Parameters' + Style.END + f': "{script_name}" at state "{script_state}" loaded:')
    pp(script_param, compact=True)
    print()

    return path_iput, path_oput, script_param


def get_relative_path(path, levels=4):
    temp = os.path.join(path, *list([os.pardir]*levels))
    return os.path.relpath(path, temp)

def stylized_script_start(script_end: str):
    print(Style.BGD_WHT + Style.CAUTION + f' "{script_end}" '.center(80, '*') + Style.ENDD)

def stylized_script_end(script_end: str):
    print(Style.BGD_BLK + Style.WHITE + f' "{script_end}" Done '.center(80, '*') + Style.ENDD)


if __name__ == '__main__':
    Style().inter_code()