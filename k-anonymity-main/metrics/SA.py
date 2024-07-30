#metrique pour mesurer la répartition des attributs sensibles dans les classes d'équivalence
#3 degrés de tolérences : soit 100% des SA sont les mêmes dans les EQ soit 90% soit 80%


class metric_sa:
    
    def __init__(self, anon_data, qi_index, sa_index, threshold) -> None:
        self.anon_data = anon_data
        self.qi_index = qi_index
        self.sa_index = sa_index
        self.num_qi = len(qi_index)
        self.threshold=threshold
        # print("sa",sa_index)
        # print(anon_data)
        
    def calculate_equivalence_classes(self):
        from collections import defaultdict, Counter
        
        # Dictionary to hold equivalence classes
        equivalence_classes = defaultdict(list)
        
        # Group rows by quasi-identifiers
        for row in self.anon_data:
            qi_values = tuple(row[index] for index in self.qi_index)
            #print("qi_values",qi_values) #recupérer les valeurs des QIDs 
            equivalence_classes[qi_values].append(row)
        #print("equivalence_classes.items()",equivalence_classes.items()) #dictionnaire des lignes avec {[(combi_QIDs),[lignes associées]],...}
        
        # Calculate sensitive attribute distributions for each class
        sensitive_distributions = {}
        #pour chaque classe d'équivalence, on récupère les sa_values
        for qi_values, rows in equivalence_classes.items():
            #print("qi_values",qi_values)
            sa_values = [row[self.sa_index] for row in rows] #liste des sa par EQ
            #print("sa_values",sa_values)
            count = Counter(sa_values) #dico {sa_val:nbre de fois dans la EQ,...} pour tous les sa de la classe
            total = len(sa_values) #nombre d'éléments dans la EQ
            #print("count,total",count,total)
            distribution = {sa: (count[sa] / total) * 100 for sa in count} #dico {sa_value:% de présence dans la EQ} pour tous les sa de la classe
            #print("distribution",distribution)
            sensitive_distributions[qi_values] = distribution
            #print("sensitive_distributions[qi_values]",sensitive_distributions[qi_values])
        # Number of equivalence classes
        num_equivalence_classes = len(equivalence_classes)
        # print("num_equivalence_classes",num_equivalence_classes)
        # print("sensitive_distributions",sensitive_distributions) #dico avec {(combi_qi): {sa:%...},...}
        return sensitive_distributions, num_equivalence_classes
    
    def compute_score(self):
        # Get sensitive distributions and number of equivalence classes
        sensitive_distributions, num_equivalence_classes = self.calculate_equivalence_classes()
        # Calculate the score based on the given threshold
        score = 0
        for distribution in sensitive_distributions.values():
            if any(percentage > self.threshold for percentage in distribution.values()):
                    score+=1
        metric_sa=(score/num_equivalence_classes)
        return metric_sa
        