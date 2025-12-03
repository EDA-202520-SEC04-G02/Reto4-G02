import App.view as view


#  -------------------------------------------
import csv
import sys
csv.field_size_limit(2147483647)
default_limit = 1000
sys.setrecursionlimit(default_limit*100)
#  -------------------------------------------


# Main function
def main():
    view.main()


# Main function call to run the program
if __name__ == '__main__':
    main()
