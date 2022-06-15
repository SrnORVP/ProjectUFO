import os
import re
import sys
from pprint import pprint as pp


class Style:
    COUNT = 84
    FILL = '#'
    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    DARKCYAN = '\033[36m'
    WHITE = '\033[97m'
    BLUE = '\033[94m'
    DARKBLUE = '\033[34m'
    GREEN = '\033[92m'
    DARKGREEN = '\033[32m'
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


def stylized_print_black(string_input: str):
    print(Style.BGD_BLK + Style.BLACK + f' {string_input} '.center(Style.COUNT, Style.FILL) + Style.ENDD)


def stylized_print_caution(string_input: str):
    print(Style.BGD_BLK + Style.CAUTION + f' {string_input} '.center(Style.COUNT, Style.FILL) + Style.ENDD)


def stylized_print_green(string_input: str):
    print(Style.BGD_BLK + Style.DARKGREEN + f' {string_input} '.center(Style.COUNT, Style.FILL) + Style.ENDD)


def stylized_print_cyan(string_input: str):
    print(Style.BGD_BLK + Style.DARKCYAN + f' {string_input} '.center(Style.COUNT, Style.FILL) + Style.ENDD)


def stylized_print_blue(string_input: str):
    print(Style.BGD_BLK + Style.DARKBLUE + f' {string_input} '.center(Style.COUNT, Style.FILL) + Style.ENDD)


def stylized_print_section(string_input1: str, string_input2: str):
    print(Style.UDLBLD + string_input1 + Style.END + string_input2)


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
        print(f'File found: "{get_rel_path(strTemp)}"')
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
            print(f'File found: "{get_rel_path(strPathOP)}"')
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
            stylized_print_caution('Similar file(s) are DELETED')
        else:
            stylized_print_caution('Similar file(s) are NOT deleted')


def check_file_path_exist(path: str, create: bool = True):
    path = os.path.join(*path)
    if not os.path.exists(path):
        os.mkdir(path)
        print(f'Dir not found, now created: {path}')


def get_rel_path(path, levels=4):
    temp = os.path.join(path, *list([os.pardir]*levels))
    return os.path.relpath(path, temp)


def verify_pathname(pathname, verify_exist=True, verbose=False):
    state = True
    while state:
        exist = os.path.exists(pathname)
        state = not exist if verify_exist else exist
    if verbose:
        print(f'Verified {pathname} state.\n')

# ----------------------------------------------------------------------------------------------------------------------


if __name__ == '__main__':
    Style().inter_code()
