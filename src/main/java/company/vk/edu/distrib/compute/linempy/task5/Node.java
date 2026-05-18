package company.vk.edu.distrib.compute.linempy.task5;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Класс Node реализует логику узла в распределенной системе с использованием алгоритма Bully.
 * Включает механизмы детекции отказов, проведения выборов и логирования времени работы алгоритма.
 */
public class Node implements Runnable {
    private static final long PING_INTERVAL = 3000;
    private static final long PING_TIMEOUT = 5000;
    private static final long ELECTION_TIMEOUT = 2000;
    private static final long MIN_ELECTION_INTERVAL = 3000;

    private static final String ELECT_MSG = "ELECT";
    private static final String LEADER_MSG = "LEADER";
    private static final String NEW_LEADER_MSG = "NEW LEADER";
    private static final String PING_MSG = "PING";
    private static final String ANSWER_MSG = "ANSWER";
    private static final String WARN_MSG = "WARN";
    private static final String DOWN_MSG = "DOWN";
    private static final String RESTORE_MSG = "RESTORE";
    private static final String SHUTDOWN_MSG = "SHUTDOWN";

    private static final String RESET = "\u001B[0m";
    private static final String RED = "\u001B[31m";
    private static final String GREEN = "\u001B[32m";
    private static final String YELLOW = "\u001B[33m";
    private static final String CYAN = "\u001B[36m";
    private static final String PURPLE = "\u001B[35m";

    private final int id;
    private final Map<Integer, Node> cluster;
    private final BlockingQueue<Message> messageQueue;
    private final AtomicBoolean active;
    private final AtomicBoolean electionInProgress;
    private final AtomicLong lastAnswerReceived;
    private final AtomicLong lastElectionTime;
    private final ScheduledExecutorService scheduler;

    private int currentLeaderId;
    private ScheduledFuture<?> electionTimeoutTask;
    private final ReentrantLock electionLock = new ReentrantLock();

    public Node(int id, Map<Integer, Node> cluster) {
        this.id = id;
        this.cluster = cluster;
        this.messageQueue = new LinkedBlockingQueue<>();
        this.active = new AtomicBoolean(true);
        this.electionInProgress = new AtomicBoolean(false);
        this.lastAnswerReceived = new AtomicLong(System.currentTimeMillis());
        this.lastElectionTime = new AtomicLong(0);
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.currentLeaderId = -1;
    }

    public int getId() {
        return id;
    }

    public boolean isActive() {
        return active.get();
    }

    public int getCurrentLeaderId() {
        return currentLeaderId;
    }

    @SuppressWarnings({"SystemPrintln", "PMD.SystemPrintln"})
    private void log(String level, String msg, String color) {
        if (isLoggingEnabled()) {
            System.out.printf("%s[Node %d] [%-8s] %s%s\n", color, id, level, msg, RESET);
        }
    }

    @SuppressWarnings({"SystemPrintln", "PMD.SystemPrintln"})
    private void logWithDuration(String level, String msg, String color, long duration) {
        if (isLoggingEnabled()) {
            System.out.printf("%s[Node %d] [%-8s] %s (duration: %d ms)%s\n",
                    color, id, level, msg, duration, RESET);
        }
    }

    private boolean isLoggingEnabled() {
        return true;
    }

    public void setStatus(boolean status) {
        if (status && !active.get()) {
            active.set(true);
            log(RESTORE_MSG, "Node recovered and rejoins the cluster.", GREEN);
            startElection();
        } else if (!status && active.get()) {
            active.set(false);
            log(DOWN_MSG, "Node crashed (failure).", RED);
            cancelElectionTimeout();
        }
    }

    public void receiveMessage(Message msg) {
        if (active.get()) {
            messageQueue.offer(msg);
        }
    }

    @Override
    public void run() {
        scheduler.scheduleAtFixedRate(this::pingLeader, 2, PING_INTERVAL / 1000, TimeUnit.SECONDS);
        processMessages();
    }

    private void processMessages() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                Message msg = messageQueue.poll(500, TimeUnit.MILLISECONDS);
                if (msg != null && active.get()) {
                    handleMessage(msg);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            scheduler.shutdownNow();
        }
    }

    private void handleMessage(Message msg) {
        final MessageType type = msg.type();
        if (type == MessageType.ELECT) {
            handleElect(msg);
        } else if (type == MessageType.ANSWER) {
            handleAnswer(msg);
        } else if (type == MessageType.VICTORY) {
            handleVictory(msg);
        } else if (type == MessageType.PING) {
            handlePing(msg);
        }
    }

    private void handleElect(Message msg) {
        if (msg.senderId() < this.id) {
            log(ELECT_MSG, "Received ELECT from " + msg.senderId() + ". Sending ANSWER.", YELLOW);
            sendTo(msg.senderId(), new Message(MessageType.ANSWER, this.id));

            if (!electionInProgress.get()) {
                startElection();
            }
        }
    }

    private void handleAnswer(Message msg) {
        log(ANSWER_MSG, "Received ANSWER from node " + msg.senderId() + ".", CYAN);
        cancelElectionTimeout();
        electionInProgress.set(false);
        lastAnswerReceived.set(System.currentTimeMillis());
    }

    private void handleVictory(Message msg) {
        this.currentLeaderId = msg.senderId();
        this.electionInProgress.set(false);
        cancelElectionTimeout();
        lastAnswerReceived.set(System.currentTimeMillis());
        log(NEW_LEADER_MSG, "Node " + msg.senderId() + " is now the leader.", PURPLE);
    }

    private void handlePing(Message msg) {
        if (this.id == currentLeaderId) {
            sendTo(msg.senderId(), new Message(MessageType.ANSWER, this.id));
        }
    }

    private void startElection() {
        electionLock.lock();
        try {
            if (!active.get() || electionInProgress.get()) {
                return;
            }

            final long now = System.currentTimeMillis();
            final long elapsed = now - lastElectionTime.get();
            if (elapsed < MIN_ELECTION_INTERVAL) {
                log(ELECT_MSG, "Too frequent elections, skipping.", YELLOW);
                return;
            }
            lastElectionTime.set(now);

            final long electionStartTime = now;
            log(ELECT_MSG, "Starting leader election (Bully algorithm)...", YELLOW);
            electionInProgress.set(true);
            cancelElectionTimeout();

            final boolean higherExists = checkHigherNodes();
            if (higherExists) {
                scheduleElectionTimeout(electionStartTime);
            } else {
                becomeLeader(electionStartTime);
            }
        } finally {
            electionLock.unlock();
        }
    }

    private boolean checkHigherNodes() {
        boolean higherExists = false;
        final Message electMsg = new Message(MessageType.ELECT, this.id);
        for (Integer otherId : cluster.keySet()) {
            final Node otherNode = cluster.get(otherId);
            if (otherId > this.id && otherNode != null && otherNode.isActive()) {
                higherExists = true;
                sendTo(otherId, electMsg);
                log(ELECT_MSG, "Sent ELECT to node " + otherId, CYAN);
            }
        }
        return higherExists;
    }

    private void becomeLeader(final long startTime) {
        if (!active.get()) {
            return;
        }

        final long duration = System.currentTimeMillis() - startTime;
        this.currentLeaderId = this.id;
        this.electionInProgress.set(false);
        cancelElectionTimeout();
        lastAnswerReceived.set(System.currentTimeMillis());

        logWithDuration(LEADER_MSG, "I AM THE NEW LEADER!", GREEN, duration);

        final Message victoryMsg = new Message(MessageType.VICTORY, this.id);
        for (Node node : cluster.values()) {
            if (node.getId() != this.id && node.isActive()) {
                node.receiveMessage(victoryMsg);
            }
        }
    }

    @SuppressWarnings({"SystemPrintln", "PMD.SystemPrintln"})
    private void scheduleElectionTimeout(final long startTime) {
        electionTimeoutTask = scheduler.schedule(() -> {
            if (active.get() && electionInProgress.get()) {
                final long duration = System.currentTimeMillis() - startTime;
                if (isLoggingEnabled()) {
                    System.out.printf("%s[Node %d] [%-8s] Higher nodes did not respond. Becoming leader. "
                                    + "(duration: %d ms)%s\n",
                            YELLOW, id, ELECT_MSG, duration, RESET);
                }
                becomeLeader(startTime);
            }
        }, ELECTION_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    private void pingLeader() {
        if (!active.get() || id == currentLeaderId) {
            return;
        }

        if (currentLeaderId == -1) {
            startElection();
            return;
        }

        final Node leaderNode = cluster.get(currentLeaderId);
        final boolean leaderNotActive = leaderNode == null || !leaderNode.isActive();
        if (leaderNotActive) {
            log(WARN_MSG, "Leader " + currentLeaderId + " is not active!", RED);
            startElection();
            return;
        }

        final long now = System.currentTimeMillis();
        final boolean pingTimeoutExpired = now - lastAnswerReceived.get() > PING_TIMEOUT;
        if (pingTimeoutExpired) {
            log(WARN_MSG, "Leader " + currentLeaderId + " did not respond to PING!", RED);
            startElection();
        } else {
            sendTo(currentLeaderId, new Message(MessageType.PING, this.id));
            log(PING_MSG, "Sent PING to leader " + currentLeaderId, CYAN);
        }
    }

    private void sendTo(int targetId, Message msg) {
        final Node target = cluster.get(targetId);
        if (target != null) {
            target.receiveMessage(msg);
        }
    }

    private void cancelElectionTimeout() {
        if (electionTimeoutTask != null && !electionTimeoutTask.isDone()) {
            electionTimeoutTask.cancel(false);
        }
    }

    public void stop() {
        log(SHUTDOWN_MSG, "Graceful shutdown initiated...", RED);
        if (id == currentLeaderId) {
            final Message electMsg = new Message(MessageType.ELECT, this.id);
            for (Node node : cluster.values()) {
                if (node.getId() != this.id && node.isActive()) {
                    node.receiveMessage(electMsg);
                }
            }
        }
        active.set(false);
        scheduler.shutdownNow();
    }
}
