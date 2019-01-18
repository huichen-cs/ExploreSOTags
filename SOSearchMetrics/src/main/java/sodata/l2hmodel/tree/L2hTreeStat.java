package sodata.l2hmodel.tree;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class L2hTreeStat {
    private long totalObs;
    private int numNodes;
    private Map<Integer, Long> totalObsPerLevel;
    private Map<Integer, Integer> numNodesPerLevel;
    
    public L2hTreeStat() {
        totalObsPerLevel = new HashMap<Integer, Long>();
        numNodesPerLevel = new HashMap<Integer, Integer>();
    }
    
    public L2hTreeStat(L2hTreeStat stat) {
        this();
        
        this.totalObs = stat.totalObs;
        this.numNodes = stat.numNodes;

        stat.totalObsPerLevel.forEach((k, v) -> this.totalObsPerLevel.put(k, v));
        stat.numNodesPerLevel.forEach((k, v) -> this.numNodesPerLevel.put(k, v));
    }

    public void setTotalObs(int totalObs) {
        this.totalObs = totalObs;
    }

    public void setNumNodes(int numNodes) {
        this.numNodes = numNodes;
    }
    
    public void setNumNodesAtLevel(int level, int numNodes) {
        numNodesPerLevel.put(level, numNodes);
    }
    
    public void setTotalObsAtLevel(int level, long totalObs) {
        totalObsPerLevel.put(level, totalObs);
    }
    
    public void printStat(PrintStream writer) {
//      >>> # observations = 21533574
//      >>> # nodes = 322
//      >>> level 0. n: 1. o: 2941366
//      >>> level 1. n: 4. o: 3451008
//      >>> level 2. n: 221. o: 11833589
//      >>> level 3. n: 69. o: 2484924
//      >>> level 4. n: 19. o: 665076
//      >>> level 5. n: 7. o: 147662
//      >>> level 6. n: 1. o: 9949

        writer.println(">>> # observations = " + totalObs);
        writer.println(">>> # nodes = " + numNodes);
        SortedSet<Integer> keySet = new TreeSet<Integer>((lhs, rhs) -> Integer.compare(lhs, rhs));
        keySet.addAll(totalObsPerLevel.keySet());
        keySet.addAll(numNodesPerLevel.keySet());
        keySet.forEach(k -> writer.format(">>> level %d. n: %d. o: %d%n", k, numNodesPerLevel.get(k),
                totalObsPerLevel.get(k)));
    }
}
