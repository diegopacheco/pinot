package com.linkedin.thirdeye.taskexecution.impl.operatordag;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.taskexecution.dag.Node;
import com.linkedin.thirdeye.taskexecution.dag.NodeIdentifier;
import com.linkedin.thirdeye.taskexecution.executor.ExecutionStatus;
import com.linkedin.thirdeye.taskexecution.operator.Operator;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * A OperatorNode that executes work using one partition.
 */
public class OperatorNode implements Node<OperatorNode, Channel> {

  protected NodeIdentifier nodeIdentifier = new NodeIdentifier();
  private Operator operator;

  private Set<OperatorNode> incomingNode = new HashSet<>();
  private Set<OperatorNode> outgoingNode = new HashSet<>();
  private Set<Channel> incomingEdge = new HashSet<>();
  private Set<Channel> outgoingEdge = new HashSet<>();

  public OperatorNode(Operator operator) {
    setIdentifier(operator.getNodeIdentifier());
    this.operator = operator;
  }

  public NodeIdentifier getIdentifier() {
    return nodeIdentifier;
  }

  public void setIdentifier(NodeIdentifier nodeIdentifier) {
    Preconditions.checkNotNull(nodeIdentifier);
    this.nodeIdentifier = nodeIdentifier;
  }

  public void addIncomingNode(OperatorNode node) {
    Preconditions.checkNotNull(node);
    Preconditions.checkArgument(node != this);
    incomingNode.add(node);
  }

  public void addOutgoingNode(OperatorNode node) {
    Preconditions.checkNotNull(node);
    Preconditions.checkArgument(node != this);
    outgoingNode.add(node);
  }

  public Set<OperatorNode> getIncomingNodes() {
    return incomingNode;
  }

  public Set<OperatorNode> getOutgoingNodes() {
    return outgoingNode;
  }

  @Override
  public void addIncomingEdge(Channel edge) {
    Preconditions.checkNotNull(edge);
    incomingEdge.add(edge);
  }

  @Override
  public void addOutgoingEdge(Channel edge) {
    Preconditions.checkNotNull(edge);
    outgoingEdge.add(edge);
  }

  @Override
  public Set<Channel> getIncomingEdges() {
    return incomingEdge;
  }

  @Override
  public Set<Channel> getOutgoingEdges() {
    return outgoingEdge;
  }

  public Operator getOperator() {
    return operator;
  }

  public ExecutionStatus getExecutionStatus() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OperatorNode that = (OperatorNode) o;
    return Objects.equals(getOperator(), that.getOperator()) && Objects.equals(nodeIdentifier, that.nodeIdentifier)
        && Objects.equals(incomingNode, that.incomingNode) && Objects.equals(outgoingNode, that.outgoingNode) && Objects
        .equals(incomingEdge, that.incomingEdge) && Objects.equals(outgoingEdge, that.outgoingEdge);
  }

  @Override
  public int hashCode() {
    return Objects.hash(nodeIdentifier);
  }
}