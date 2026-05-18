package company.vk.edu.distrib.compute.linempy.task5;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class BullyAlgorithmSystem {

    private static final int NODE_COUNT = 5;
    private static final String PMD_SUPPRESS = "PMD.SystemPrintln";

    private BullyAlgorithmSystem() {
    }

    public static void main(String[] args) throws InterruptedException {
        final Map<Integer, Node> nodes = new ConcurrentHashMap<>();
        final ExecutorService threadPool = Executors.newFixedThreadPool(NODE_COUNT);

        printMessage("=== INITIALIZING CLUSTER WITH " + NODE_COUNT + " NODES ===");
        printParameters();

        createAndStartNodes(nodes, threadPool);

        Thread.sleep(5000);
        printStatus(nodes);

        runLeaderFailureTest(nodes);
        Thread.sleep(8000);
        printStatus(nodes);

        runNodeRecoveryTest(nodes);
        Thread.sleep(8000);
        printStatus(nodes);

        runGracefulShutdownTest(nodes);
        Thread.sleep(5000);
        printStatus(nodes);

        runRandomFailuresTest(nodes);
        Thread.sleep(10000);
        printStatus(nodes);

        printMessage("\n=== SIMULATION COMPLETE ===");
        stopAllNodes(nodes);
        threadPool.shutdownNow();
    }

    @SuppressWarnings({"SystemPrintln", PMD_SUPPRESS})
    private static void printParameters() {
        System.out.println("Parameters: pingInterval=3s, pingTimeout=5s, electionTimeout=2s\n");
    }

    @SuppressWarnings("ForLoopReplaceableByForEach")
    private static void createAndStartNodes(Map<Integer, Node> nodes, ExecutorService threadPool) {
        for (int i = 1; i <= NODE_COUNT; i++) {
            final Node node = new Node(i, nodes);
            nodes.put(i, node);
        }
        nodes.values().forEach(threadPool::submit);
    }

    private static void runLeaderFailureTest(Map<Integer, Node> nodes) {
        printTestHeader("TEST 1: LEADER FAILURE (Node 5)");
        nodes.get(5).setStatus(false);
    }

    private static void runNodeRecoveryTest(Map<Integer, Node> nodes) {
        printTestHeader("TEST 2: NODE RECOVERY (Node 5)");
        nodes.get(5).setStatus(true);
    }

    private static void runGracefulShutdownTest(Map<Integer, Node> nodes) {
        printTestHeader("TEST 3: GRACEFUL SHUTDOWN (Current Leader)");
        final int currentLeader = findLeader(nodes);
        if (currentLeader != -1) {
            nodes.get(currentLeader).stop();
        }
    }

    @SuppressWarnings({"SystemPrintln", PMD_SUPPRESS})
    private static void runRandomFailuresTest(Map<Integer, Node> nodes) throws InterruptedException {
        printTestHeader("TEST 4: RANDOM FAILURES AND RECOVERIES");
        final int[] failOrder = {3, 1, 4};
        for (int id : failOrder) {
            final Node node = nodes.get(id);
            if (node != null && node.isActive()) {
                System.out.printf(">>> Node %d is failing...\n", id);
                node.setStatus(false);
                Thread.sleep(3000);
                System.out.printf(">>> Node %d is recovering...\n", id);
                node.setStatus(true);
                Thread.sleep(3000);
            }
        }
    }

    private static void stopAllNodes(Map<Integer, Node> nodes) {
        for (Node node : nodes.values()) {
            node.stop();
        }
    }

    @SuppressWarnings({"SystemPrintln", PMD_SUPPRESS})
    private static void printMessage(String msg) {
        System.out.println(msg);
    }

    @SuppressWarnings({"SystemPrintln", PMD_SUPPRESS})
    private static void printTestHeader(String header) {
        System.out.println("\n--- " + header + " ---");
    }

    @SuppressWarnings({"SystemPrintln", PMD_SUPPRESS})
    private static void printStatus(Map<Integer, Node> nodes) {
        System.out.println("\n------------------------------------");
        System.out.println("CURRENT CLUSTER STATUS:");
        for (Node node : nodes.values()) {
            final String status = getNodeStatus(node);
            System.out.printf("Node %d: %-10s (leader: %d)%n",
                    node.getId(), status, node.getCurrentLeaderId());
        }
        System.out.println("------------------------------------\n");
    }

    private static String getNodeStatus(Node node) {
        if (!node.isActive()) {
            return "DOWN";
        }
        return node.getId() == node.getCurrentLeaderId() ? "LEADER" : "FOLLOWER";
    }

    private static int findLeader(Map<Integer, Node> nodes) {
        for (Node node : nodes.values()) {
            if (node.isActive() && node.getId() == node.getCurrentLeaderId()) {
                return node.getId();
            }
        }
        return -1;
    }
}
