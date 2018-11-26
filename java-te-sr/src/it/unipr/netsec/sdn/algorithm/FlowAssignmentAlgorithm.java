package it.unipr.netsec.sdn.algorithm;

/*
 *
 * @author Luca Davoli - <a href="mailto:lucadavo@gmail.com">lucadavo@gmail.com</a> - Department of Information Engineering - University of Parma
 *
 */

import it.unipr.netsec.sdn.graph.util.GraphConstant;
import it.unipr.netsec.sdn.trafficflow.element.FlowElement;
import it.unipr.netsec.sdn.trafficflow.element.TrafficFlowContainer;
import org.graphstream.algorithm.Dijkstra;
import org.graphstream.algorithm.DynamicAlgorithm;
import org.graphstream.graph.Edge;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.graph.Path;
import org.graphstream.graph.implementations.MultiGraph;
import org.graphstream.stream.SinkAdapter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Traffic Flow Assignment algorithm implementation
 */
public class FlowAssignmentAlgorithm extends SinkAdapter implements DynamicAlgorithm {

    private boolean terminate = false;

    private Graph g = null; //拓扑图
    private Graph inner = null;

    private TrafficFlowContainer flows = null;  //Traffic Flows
    private TrafficFlowContainer partialFlows = null;

    private double sigma = 0.0;
    private double Tglob = 0.0;
    private double Tfin = 0.0;

    private boolean directedEdge = true;    //是否是有向图

    /**
     * 传入拓扑图并完成相应的初始化
     *
     * @param graph
     */
    @Override
    public void init(Graph graph) {
        g = graphCopy(graph, graph.getId() + "_INIT");
        g.addSink(this);

        partialFlows = new TrafficFlowContainer();
    }

    @Override
    public void compute() {
        initialFlowAllocation();

        Tglob = averageDelayCalculation(inner);

        finalTimeAllocation(Tglob);

        boolean iterate = true;

        while (iterate) {
            for (int ii = 0; ii < partialFlows.size(); ii++) {
                flowAllocationLoop(partialFlows.get(ii));
            }

            iterate = timesComparison();
        }
    }

    @Override
    public void terminate() {
        g.removeSink(this);
        terminate = true;
    }

    /**
     * Set the traffic flows container
     *
     * @param tfc the traffic flows container
     */
    public void setTrafficFlows(TrafficFlowContainer tfc) {
        flows = tfc;
    }

    /**
     * Return a container of traffic flows, enhanced with allocated paths
     *
     * @return the allocated paths
     */
    public TrafficFlowContainer getFlowAssignment() {
        if (terminate) {
            //return flows;
            return partialFlows;
        }

        return null;
    }

    /**
     * Return the updated network
     *
     * @return the updated network
     */
    public Graph getUpdatedGraph() {
        return graphCopy(inner, inner.getId() + "_COPY");
    }

    public void setDirectedEdge(boolean directed) {
        this.directedEdge = directed;
    }

    public boolean isDirectedEdge() {
        return this.directedEdge;
    }

    /**
     * Step 2:
     * First FLOW ALLOCATION
     */
    private void initialFlowAllocation() {
        inner = graphCopy(g, g.getId() + "_INNER"); //拷贝g

        double BIGK = 0.0;  //所有链路的最大带宽

        for (Edge e : inner.getEdgeSet()) { //遍历inner的链路集
            double nominalCapacity = e.getAttribute(GraphConstant.ATTRIBUTE_EDGE_CAPACITY); //获取当前链路带宽
            BIGK = Math.max(BIGK, nominalCapacity); //求解最大带宽
        }

        HashMap<String, TrafficFlowContainer> edgesUtilization = new HashMap<>();   //构建Hash表用来存放链路带宽的使用情况
        for (Edge e : inner.getEdgeSet()) {
            String key = "(" + e.getSourceNode().getId() + "," + e.getTargetNode().getId() + ")";   //以(source,target)作为key
            edgesUtilization.put(key, new TrafficFlowContainer());  //每组(source,target)构建TrafficFlowContainer
        }

        for (FlowElement f : flows) {   //遍历Flows
            Graph innerTMP = new MultiGraph("TMPGraph");    //生成临时图innerTMP
            for (Node node : inner.getNodeSet()) {  //遍历inner的节点集
                Node n = innerTMP.addNode(node.getId());    //将inner中的点集添加到innerTMP中
                for (String attribute : node.getAttributeKeySet()) {    //遍历该点的所有属性key
                    n.addAttribute(attribute, node.getAttribute(attribute));    //将该点的所有属性添加到innerTMP中
                }
            }

            for (Edge e : inner.getEdgeSet()) { //遍历inner的链路集
                double nominalCapacity = e.getAttribute(GraphConstant.ATTRIBUTE_EDGE_CAPACITY); //获取该链路的带宽
                double load = e.getAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD);    //获取链路的负载
                double residualCapacity = nominalCapacity - load;   //剩余带宽=带宽-负载

                double linkCost = BIGK / residualCapacity;  //链路的权值=最大带宽/剩余带宽

                e.addAttribute(GraphConstant.ATTRIBUTE_EDGE_COST, linkCost);    //链路添加权值属性

                double flowBW = f.getBandwidth();   //获取当前flow的带宽
                if ((residualCapacity - flowBW) >= 0.0) {   //如果剩余带宽比flow的带宽来的大
                    Edge eTmp = innerTMP.addEdge(e.getId(), e.getSourceNode().getId(), e.getTargetNode().getId(), this.directedEdge);   //则将该链路添加到innerTMP中去
                    for (String attribute : e.getAttributeKeySet()) {
                        eTmp.addAttribute(attribute, e.getAttribute(attribute));    //同时将该链路的全部属性拷贝过去
                    }
                }
            }

            Node source = innerTMP.getNode(f.getNodeSource());  //获取当前flow的source节点

			/*
			  构造Dijkstra对象
			  该Dijkstra是以source生成的Shortest Path Tree
			  以链路权值作为长度
			 */
            Dijkstra d = new Dijkstra(Dijkstra.Element.EDGE, "result", GraphConstant.ATTRIBUTE_EDGE_COST);
            d.init(innerTMP);   //把innerTMP带入并初始化
            d.setSource(source);    //设置source节点
            d.compute();    //计算

            Node dest = innerTMP.getNode(f.getNodeDestination());   //获取当前flow的destination节点

            String path = "";
            for (Edge pathEdge : d.getPathEdges(dest)) {    //遍历从source到destination的最短路径上的链路
                double load = inner.getEdge(pathEdge.getId()).getAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD);    //获取当前边的负载
                load += f.getBandwidth();    //负载加上flow带宽
                inner.getEdge(pathEdge.getId()).addAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD, load);    //inner中对应的边更新负载
                String key = "(" + pathEdge.getSourceNode().getId() + "," + pathEdge.getTargetNode().getId() + ")";
                path = "," + key + path;    //将该边加入到path中
                edgesUtilization.get(key).add(f);    //hash表中存储对应的链路带宽使用情况
            }

            f.setPath(d.getPath(dest));    //对应的flow设置path

            if (f.getPath().size() > 0) {    //如果该flow的path长度大于0
                partialFlows.add(f);    //将该flow加入到partialFlows
            }

            //清除innerTMP缓存
            innerTMP.clearSinks();
            innerTMP.clearElementSinks();
            innerTMP.clearAttributeSinks();
            innerTMP.clearAttributes();
            innerTMP.clear();
        }

        for (Edge e : g.getEdgeSet()) {        //遍历g的链路集
            String key = "(" + e.getSourceNode().getId() + "," + e.getTargetNode().getId() + ")";    //获取key
            if (edgesUtilization.get(key) != null) {    //如果该链路已经是使用过
                double count = 0;
                for (FlowElement fe : edgesUtilization.get(key)) {
                    count += fe.getBandwidth();     //则求出该链路上的总带宽
                }
                e.addAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD, count);   //更新该链路的负载
            }
        }
    }

    /**
     * Step 3:
     * Delay calculation on provided graph, as T_glob
     *
     * @param graph Graph on which calculate average delay, as T_glob
     * @return T_glob = average delay
     */
    private double averageDelayCalculation(Graph graph) {
        sigma = 0.0;
		
		/*
		for (FlowElement f : flows) {
			sigma += f.getBandwidth();
		}
		*/
        for (FlowElement f : partialFlows) {
            sigma += f.getBandwidth();
        }

        double localTglob = 0.0;
        for (Edge e : graph.getEdgeSet()) {
            double capacity = e.getAttribute(GraphConstant.ATTRIBUTE_EDGE_CAPACITY);
            double load = e.getAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD);
            localTglob += load / (capacity - load);
        }

        localTglob = (1 / sigma) * localTglob;

        return localTglob;
    }

    /**
     * Step 4:
     * Assign to Tfin a T_glob value
     *
     * @param glob the T_glob value that has to be assigned to T_final
     */
    private void finalTimeAllocation(double glob) {
        Tfin = glob;
    }

    /**
     * Steps 5-6-7:
     * Flow allocation loop, to test if exists others available allocations for the provided flows.
     *
     * @param cfe Flow element that has to be eventually re-allocated, exploiting the algorithm
     */
    private void flowAllocationLoop(FlowElement cfe) {
        Graph tmp = graphCopy(inner, inner.getId() + "_STEP567");

        // Deallocate current flow from temporary graph
        for (Edge e : cfe.getPath().getEdgeSet()) {
            double load = tmp.getEdge(e.getId()).getAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD);
            load = load - cfe.getBandwidth();
            tmp.getEdge(e.getId()).addAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD, load);
        }

        // Prune edges that doesn't support the current flow
        ArrayList<Edge> pruned = new ArrayList<Edge>();
        for (Edge e : tmp.getEdgeSet()) {
            double capacity = e.getAttribute(GraphConstant.ATTRIBUTE_EDGE_CAPACITY);
            double load = e.getAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD);
            load += cfe.getBandwidth();
            if (capacity <= load) {
                pruned.add(e);
                tmp.removeEdge(e);
            }
        }

        // Edge length calculation, with the UniRoma2 formula
        for (Edge e : tmp.getEdgeSet()) {
            double capacity = e.getAttribute(GraphConstant.ATTRIBUTE_EDGE_CAPACITY);
            double load = e.getAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD);
            double length = capacity / (Math.pow(capacity - load, 2));
            length = (1 / sigma) * length;
            e.addAttribute(GraphConstant.ATTRIBUTE_EDGE_LENGTH, length);
        }

        Node source = tmp.getNode(cfe.getNodeSource());

        // CSPF --> Dijkstra
        Dijkstra d = new Dijkstra(Dijkstra.Element.EDGE, "result", GraphConstant.ATTRIBUTE_EDGE_LENGTH);
        d.init(tmp);
        d.setSource(source);
        d.compute();

        Node dest = tmp.getNode(cfe.getNodeDestination());

        int count = 0;
        for (Path p : d.getAllPaths(dest)) {
            count++;
        }

        Path path = cfe.getPath();
        double pathLength = 0.0;
        for (Edge e : path.getEdgeSet()) {
            pathLength += (double) tmp.getEdge(e.getId()).getAttribute(GraphConstant.ATTRIBUTE_EDGE_LENGTH);
        }

        if (count > 1) {
            for (Path p : d.getAllPaths(dest)) {
                double pl = 0.0;
                for (Edge e : p.getEdgeSet()) {
                    pl += (double) tmp.getEdge(e.getId()).getAttribute(GraphConstant.ATTRIBUTE_EDGE_LENGTH);
                }

                if ((pl < pathLength) && (!pathEquals(p, path))) {
                    path = p;
                    pathLength = pl;
                }
            }

            // Sum discovered path to temporary graph
            for (Edge e : path.getEdgeSet()) {
                double load = tmp.getEdge(e.getId()).getAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD);
                load += cfe.getBandwidth();
                tmp.getEdge(e.getId()).addAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD, load);
            }

            // BEFORE re-calculate the delay time, re-add previously pruned edges to temporary graph
            for (Edge e : pruned) {
                Edge ne = tmp.addEdge(e.getId(), e.getSourceNode(), e.getTargetNode(), this.directedEdge);
                for (String keyAttribute : e.getAttributeKeySet()) {
                    ne.addAttribute(keyAttribute, e.getAttribute(keyAttribute));
                }
            }

            double localTglob = averageDelayCalculation(tmp);
            if (localTglob < Tfin) {
                finalTimeAllocation(localTglob);

                // Delete OLD path for the current flow from the graph
                for (Edge e : cfe.getPath().getEdgeSet()) {
                    double load = inner.getEdge(e.getId()).getAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD);
                    load -= cfe.getBandwidth();
                    inner.getEdge(e.getId()).addAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD, load);
                }

                // Add NEW path for the current flow to the graph
                for (Edge e : path.getEdgeSet()) {
                    double load = inner.getEdge(e.getId()).getAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD);
                    load += cfe.getBandwidth();
                    inner.getEdge(e.getId()).addAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD, load);
                }

                // Add NEW path to the current flow
                cfe.setPath(path);
            }
        } else {
            Path found = d.getPath(dest);
            double dijkstraPathLength = 0.0;
            for (Edge e : found.getEdgeSet()) {
                dijkstraPathLength += (double) e.getAttribute(GraphConstant.ATTRIBUTE_EDGE_LENGTH);
            }

            if (dijkstraPathLength < pathLength) {
                // Sum discovered path to temporary graph
                for (Edge e : found.getEdgeSet()) {
                    double load = tmp.getEdge(e.getId()).getAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD);
                    load += cfe.getBandwidth();
                    tmp.getEdge(e.getId()).addAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD, load);
                }

                // BEFORE re-calculate the delay time, re-add previously pruned edges to temporary graph
                for (Edge e : pruned) {
                    Edge ne = tmp.addEdge(e.getId(), e.getSourceNode(), e.getTargetNode(), this.directedEdge);
                    for (String keyAttribute : e.getAttributeKeySet()) {
                        ne.addAttribute(keyAttribute, e.getAttribute(keyAttribute));
                    }
                }

                double localTglob = averageDelayCalculation(tmp);
                if (localTglob < Tfin) {
                    finalTimeAllocation(localTglob);

                    // Delete OLD path for the current flow from the graph
                    for (Edge e : path.getEdgeSet()) {
                        double load = inner.getEdge(e.getId()).getAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD);
                        load -= cfe.getBandwidth();
                        inner.getEdge(e.getId()).addAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD, load);
                    }

                    // Add NEW path for the current flow to the graph
                    for (Edge e : found.getEdgeSet()) {
                        double load = inner.getEdge(e.getId()).getAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD);
                        load += cfe.getBandwidth();
                        inner.getEdge(e.getId()).addAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD, load);
                    }

                    // Add NEW path to the current flow
                    cfe.setPath(found);
                    path = found;
                    pathLength = dijkstraPathLength;
                }
            }
        }
    }

    /**
     * Step 8:
     * Comparison between actual T_fin time and T_glob time.
     *
     * @return TRUE if t_fin is lower than T_glob, FALSE otherwise.
     */
    private boolean timesComparison() {
        if (Tfin < Tglob) {
            Tglob = Tfin;
            return true;
        } else if (Tfin == Tglob) {
            return false;
        }

        return true;
    }

    private static boolean pathEquals(Path p1, Path p2) {
        if (p1.size() != p2.size()) {
            return false;
        }

        Iterator<Edge> p1_i = p1.getEdgeIterator();
        Iterator<Edge> p2_i = p2.getEdgeIterator();
        while (p1_i.hasNext()) {
            if (!p1_i.next().getId().equals(p2_i.next().getId())) {
                return false;
            }
        }

        return true;
    }

    private Graph graphCopy(Graph graph, String name) {
        Graph cpy = new MultiGraph(name);

        for (Node node : graph.getNodeSet()) {
            Node n = cpy.addNode(node.getId());
            for (String attribute : node.getAttributeKeySet()) {
                n.addAttribute(attribute, node.getAttribute(attribute));
            }
        }

        for (Edge edge : graph.getEdgeSet()) {
            Edge e = cpy.addEdge(edge.getId(), edge.getSourceNode().getId(), edge.getTargetNode().getId(), this.directedEdge);
            for (String attribute : edge.getAttributeKeySet()) {
                e.addAttribute(attribute, edge.getAttribute(attribute));
            }
        }

        return cpy;
    }

}
