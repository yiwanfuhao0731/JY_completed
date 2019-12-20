'''
Created on 2019年7月23日

@author: user
'''
from anytree import Node,RenderTree,LevelOrderIter
import pandas as pd
from gevent.ares import node

class componentNode(Node):
    separator = "-->"

class bigTree():
    def __init__(self):
        self.nodes = {}
        
    def add_node(self,new_node):
        """
        add new node to the nodes dictionary. passing parent arg as the key to the parent node
        the key of the newly added node being the name attribute of it
        """
        if not new_node.parent_key is None:
            self.nodes[new_node.name] = new_node
            self.nodes[new_node.name].parent = self.nodes[new_node.parent_key]
        else:
            self.nodes[new_node.name] = new_node
    
    def get_all_root(self):
        all_root = []
        for i,n in self.nodes.items():
            if n.parent is None:
                all_root.append(n)
        return all_root
    
    
        

df = pd.read_csv('10100118.csv',index_col = 0,header = 0).tail(2)

new_wf = {'df1' : df.iloc[:,[0]],
          'df2' : df.iloc[:,[1]],
          'df3' : df.iloc[:,[2]],
          'df4' : df.iloc[:,[5]],
          'df5' : df.iloc[:,[6]],
          'df6' : df.iloc[:,[7]],
          'df7' : df.iloc[:,[8]],
          'df8' : df.iloc[:,[9]],
          'df9' : df.iloc[:,[10]],
          'df10' : df.iloc[:,[11]],
          'df11' : df.iloc[:,[12]],
          }

gdp_tree = bigTree()

# component node dictionary list to assembly and pass in the node as bulk. name is the name of the node, also used 
# as the legend for the future plotting, raw is the
# key of the dataframe in new_wf.df, for_plot is the key in the new_wf.df, for plotting 
componentNodedict = {
    'gdp':{'name':'gdp',
          'raw':'df1',
          'for_plot':'df2'},
    'consumption':{'name':'consumption',
                   'raw':'df3',
                   'for_plot':'df4',
                   'parent_key':'gdp'},
    'investment':{'name':'investment',
                  'raw':'df5',
                  'for_plot':'df6',
                  'parent_key':'gdp'}
    }

def run_build_tree(node_info_dict,data_dict,the_tree):
    for k,v in node_info_dict.items():
        name = v['name']
        df_raw = data_dict[v['raw']]
        df_raw2 = data_dict[v['raw2']]
        parent_key = v['parent_key'] if 'parent_key' in v.keys() else None
        this_node = componentNode(name,raw=df_raw,raw2 = df_raw2,parent_key = parent_key)
        the_tree.add_node(this_node)

def add_zto_its_parent(some_root):
    for node in [n for n in LevelOrderIter(some_root)][1:]
        # map raw1 
        df_comb = pd.merge(node.raw,node.parent.raw,left_index=True,right_index=True,how='inner')
        new_name = node.raw.columns[0]+'(mapped)'
        df_comb = SM.zto(df_comb,mean_type = 'rolling',sd_type = 'rolling',roll_mean = 20*4,rolling_sd = 20*4 ,new_name = new_name)
        node.raw_mapped = df_comb[[new_name]]
        # map raw2
        df_comb = pd.merge(node.raw2,node.parent.raw2,left_index=True,right_index=True,how='inner')
        new_name = node.raw2.columns[0]+'(mapped)'
        df_comb = SM.zto(df_comb,mean_type = 'rolling',sd_type = 'rolling',roll_mean = 20*4,rolling_sd = 20*4 ,new_name = new_name)
        node.raw2_mapped = df_comb[[new_name]]

def expand_tree_below_a_node_and_return_all_parent_child_pairs(some_root):
    #return parent,child pairs for all such pairs in a tree: e.g. A-->(B,C), return (A,B),(A,C), also the name pairs
    df_res = []
    # name is used in the legend
    name_res = []
    node_expand = [n for n in LevelOrderIter(some_root)]
    for n in node_expand[1:]:
        df_res.append((n.raw_mapped,n.parent.raw))
        df_res.append((n.raw2_mapped,n.parent.raw2))
        name_res.append((n.name,n.parent.name,'6m'))
        name_res.append((n.name,n.parent.name,'6x6'))
    return df_res,name_res
            

gdp_tree = bigTree()
run_build_tree(node_info_dict=componentNodedict, data_dict=new_wf, the_tree=gdp_tree)

gdp_with_component = gdp_tree.get_all_root()
df_,name_ = gdp_tree.expand_tree_below_a_node_and_return_all_parent_child_pairs(gdp_with_component[0])
print (name_)
print ('done')

