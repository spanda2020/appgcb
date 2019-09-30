
import os
import re
import sys
import importlib
sys.path.append(os.getcwd())
#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
#current_path = os.path.abspath('.')
#parent_path = os.path.dirname(current_path)
sys.path.append('/Users/spanda/Documents/Learning/cloud/gcp/bin/')
#import util.args as arg
import app_data_clean.util.args as arg

def main():
    arg.parser.add_argument('feature_lst', help='List of features', type=str)
    arg.parser.add_argument('batch_id', help='Load id for each batch', type=int)
    arg.parser.add_argument('src_schema', help='source schema required', type=str)
    args, _ = arg.parser.parse_known_args()

    feature_nm=args.feature_lst.split(",")[0]
    batch_id=args.batch_id
    src_db=args.src_schema

    print("feature_nm : ",feature_nm)
    print("batch_id : ",batch_id)
    print("src_schema : ",src_db)


if __name__ == '__main__':
    main()
