package sodata.l2hmodel.tree;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class L2hTreeStatParser {
    private final static Logger LOGGER = LoggerFactory.getLogger(L2hTreeStatParser.class);
        
    private final String TOTAL_OBS_PATTERN = "^\\s*>>> # observations = ([0-9]*)";
    private final String NUM_NODES_PATTERN = "^\\s*>>> # nodes = ([0-9]*)";
    private final String LEVEL_STAT_PATTERN = "^\\s*>>> level ([0-9]+)\\. n: ([0-9]+)\\. o: ([0-9]*)";
    
    private Pattern totalObsPattern;
    private Pattern numNodesPattern;
    private Pattern levelStatPattern;
    
    public L2hTreeStatParser() {
        totalObsPattern = Pattern.compile(TOTAL_OBS_PATTERN);
        numNodesPattern = Pattern.compile(NUM_NODES_PATTERN);
        levelStatPattern = Pattern.compile(LEVEL_STAT_PATTERN);
    }
    
    public L2hTreeStat parse(BufferedReader reader, L2hTreeStat stat, String statLine) throws IOException {
//      >>> # observations = 21533574
//      >>> # nodes = 322
//      >>> level 0. n: 1. o: 2941366
//      >>> level 1. n: 4. o: 3451008
//      >>> level 2. n: 221. o: 11833589
//      >>> level 3. n: 69. o: 2484924
//      >>> level 4. n: 19. o: 665076
//      >>> level 5. n: 7. o: 147662
//      >>> level 6. n: 1. o: 9949
        
        LOGGER.debug("statLine: " + statLine);
        Matcher matcher = totalObsPattern.matcher(statLine);
        if (!matcher.find()) {
            throw new IllegalStateException("The line doesn't correspond to a valid statistics line: " + statLine);
        }
        String totalObsText = matcher.group(1);
        stat.setTotalObs(Integer.parseInt(totalObsText.trim()));
        
        statLine = reader.readLine();
        LOGGER.debug("statLine: " + statLine);
        if (statLine == null) {
            throw new IllegalStateException("The tree file ends prematurely");
        }
        matcher = numNodesPattern.matcher(statLine);
        if (!matcher.find()) {
            throw new IllegalStateException("The line doesn't correspond to a valid statistics line: " + statLine);
        }
        String numNodesText = matcher.group(1);
        stat.setNumNodes(Integer.parseInt(numNodesText));
        
        while ((statLine = reader.readLine()) != null) {
            LOGGER.debug("statLine: " + statLine);
            
            matcher = levelStatPattern.matcher(statLine);
            if (!matcher.find() || matcher.groupCount() < 3) {
                throw new IllegalStateException("The line doesn't correspond to a valid level statistics line: " + statLine);
            }
            LOGGER.debug("Group count: " + matcher.groupCount());

            String part = matcher.group(1);
            int level = Integer.parseInt(part.trim());
            
            part = matcher.group(2);
            int numNodesPerLevel = Integer.parseInt(part.trim());
            
            part = matcher.group(3);
            long totalObsPerLevel = Long.parseLong(part.trim());
            
            stat.setNumNodesAtLevel(level, numNodesPerLevel);
            stat.setTotalObsAtLevel(level, totalObsPerLevel);
        }
        return stat;
  }
}
