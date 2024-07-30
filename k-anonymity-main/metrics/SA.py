#metrique pour mesurer la répartition des attributs sensibles dans les classes d'équivalence
#3 degrés de tolérences : soit 100% des SA sont les mêmes dans les EQ soit 90% soit 80%


class metric_sa:
    
    def __init__(self, anon_data, qi_index, sa_index) -> None:
        self.anon_data = anon_data
        self.qi_index = qi_index
        self.sa_index = sa_index
        self.num_qi = len(qi_index)
        print("sa",sa_index)
    def compute_eq(self): #calcule les classes d'aquivalences et le nombre d'éléments par classe
        self.eq_count = {}
        for record in self.anon_data:
            qi_values = []
            for idx, qi_id in enumerate(self.qi_index):
                value = record[qi_id]
                qi_values.append(value)
            print("qi_values",qi_values)
            # Make set, because set is hashable
            eq = tuple(qi_values)
            print("eq",eq)
            # Count set of qi values
            if eq not in self.eq_count.keys():
                self.eq_count[eq] = 0
            self.eq_count[eq] += 1
            print(self.eq_count[eq])
    
    
    
    
    def compute_score(self):
        self.compute_eq()
        metric_sa = 0
        nbre_eq=0
        for eq in self.eq_count.keys():
            eq_count = self.eq_count[eq]
            print("eq_count",eq_count)
        
        return metric_sa