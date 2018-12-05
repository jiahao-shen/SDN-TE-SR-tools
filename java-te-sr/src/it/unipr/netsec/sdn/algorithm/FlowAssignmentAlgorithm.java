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

    private Graph g = null; //感觉g并没有什么用
    private Graph inner = null; //感觉inner才是主要的

    private TrafficFlowContainer flows = null;  //Traffic Flows
    private TrafficFlowContainer partialFlows = null;   //已经分配的Flows(带有Path)

    private double sigma = 0.0;
    private double Tglob = 0.0;
    private double Tfin = 0.0;

    private boolean directedEdge = true;    //是否是有向图

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
            for (FlowElement partialFlow : partialFlows) {
                flowAllocationLoop(partialFlow);
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
        inner = graphCopy(g, g.getId() + "_INNER"); //将g拷贝给inner

        double BIGK = 0.0;  //所有边的最大容量

        HashMap<String, TrafficFlowContainer> edgesUtilization = new HashMap<>();   //构建Hash表用来存放边的使用情况
        for (Edge e : inner.getEdgeSet()) { //遍历inner的边集
            double nominalCapacity = e.getAttribute(GraphConstant.ATTRIBUTE_EDGE_CAPACITY); //获取当前边的容量
            BIGK = Math.max(BIGK, nominalCapacity); //求解最大容量
            String key = "(" + e.getSourceNode().getId() + "," + e.getTargetNode().getId() + ")";   //以(source,target)作为key
            edgesUtilization.put(key, new TrafficFlowContainer());  //每组(source,target)构建TrafficFlowContainer
        }

        for (FlowElement f : flows) {   //遍历flows
            Graph innerTMP = new MultiGraph("TMPGraph");    //生成临时图innerTMP
            for (Node node : inner.getNodeSet()) {  //遍历inner的节点集
                Node n = innerTMP.addNode(node.getId());    //将inner中的节点集添加到innerTMP中
                for (String attribute : node.getAttributeKeySet()) {    //遍历该点的所有属性key
                    n.addAttribute(attribute, node.getAttribute(attribute));    //将该点的所有属性添加到innerTMP中
                }
            }

            for (Edge e : inner.getEdgeSet()) { //遍历inner的边集
                double nominalCapacity = e.getAttribute(GraphConstant.ATTRIBUTE_EDGE_CAPACITY); //获取该边的容量
                double load = e.getAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD);    //获取该边的负载
                double residualCapacity = nominalCapacity - load;   //剩余容量=容量-负载

                double linkCost = BIGK / residualCapacity;  //边的权值=最大容量/剩余剩余

                e.addAttribute(GraphConstant.ATTRIBUTE_EDGE_COST, linkCost);    //给每条边添加权值cost

                double flowBW = f.getBandwidth();   //获取当前flow的带宽
                if ((residualCapacity - flowBW) >= 0.0) {   //如果剩余容量大于flow的带宽
                    Edge eTmp = innerTMP.addEdge(e.getId(), e.getSourceNode().getId(), e.getTargetNode().getId(), this.directedEdge);   //则将该边添加到innerTMP中去
                    for (String attribute : e.getAttributeKeySet()) {
                        eTmp.addAttribute(attribute, e.getAttribute(attribute));    //同时将该边的全部属性拷贝过去
                    }
                }
            }

            Node source = innerTMP.getNode(f.getNodeSource());  //获取当前flow的source节点

            /*
             * 构造Dijkstra对象
             * 该Dijkstra是以source生成的Shortest Path Tree
             * 以边的权值作为长度
             */
            Dijkstra d = new Dijkstra(Dijkstra.Element.EDGE, "result", GraphConstant.ATTRIBUTE_EDGE_COST);
            d.init(innerTMP);   //把innerTMP带入并初始化
            d.setSource(source);    //设置source节点
            d.compute();    //计算

            Node dest = innerTMP.getNode(f.getNodeDestination());   //获取当前flow的destination节点

//            String path = "";
            for (Edge pathEdge : d.getPathEdges(dest)) {    //遍历从source到destination的最短路径上的所有边
                double load = inner.getEdge(pathEdge.getId()).getAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD);    //获取当前边的负载
                load += f.getBandwidth();    //负载加上flow带宽
                inner.getEdge(pathEdge.getId()).addAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD, load);    //inner中对应的边更新负载
                String key = "(" + pathEdge.getSourceNode().getId() + "," + pathEdge.getTargetNode().getId() + ")";
//                path = "," + key + path;    //将该边加入到path中
                edgesUtilization.get(key).add(f);    //该边的使用中添加当前flow
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

        //g貌似没有什么大用
        for (Edge e : g.getEdgeSet()) {        //遍历g的边集
            String key = "(" + e.getSourceNode().getId() + "," + e.getTargetNode().getId() + ")";    //获取(source,target)作为key
            if (edgesUtilization.get(key) != null) {    //如果该边已经使用过
                double count = 0;
                for (FlowElement fe : edgesUtilization.get(key)) {  //遍历所有经过该边的flow
                    count += fe.getBandwidth();     //则求出该边上的总带宽
                }
                e.addAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD, count);   //更新该边的负载
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

        for (FlowElement f : partialFlows) {
            sigma += f.getBandwidth();  //对partialFlows中的所有带宽求和
        }

        double localTglob = 0.0;
        for (Edge e : graph.getEdgeSet()) { //遍历graph的边集
            double capacity = e.getAttribute(GraphConstant.ATTRIBUTE_EDGE_CAPACITY);
            double load = e.getAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD);
            localTglob += load / (capacity - load); //负载除以剩余带宽求和
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
        for (Edge e : cfe.getPath().getEdgeSet()) { //遍历当前流的路径
            // 先把tmp图中的当前流删去
            double load = tmp.getEdge(e.getId()).getAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD);
            load = load - cfe.getBandwidth();
            tmp.getEdge(e.getId()).addAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD, load);
        }

        // Prune edges that doesn't support the current flow
        ArrayList<Edge> pruned = new ArrayList<>();
        for (Edge e : tmp.getEdgeSet()) {   //遍历tmp边集
            double capacity = e.getAttribute(GraphConstant.ATTRIBUTE_EDGE_CAPACITY);
            double load = e.getAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD);
            load += cfe.getBandwidth();
            if (capacity <= load) { //如果tmp的当前边加上当前流的带宽大于当前边的容量
                pruned.add(e);  //则pruned添加该边
                tmp.removeEdge(e);  //tmp删除该边
            }
        }

        // Edge length calculation, with the UniRoma2 formula
        for (Edge e : tmp.getEdgeSet()) {   //遍历tmp边集
            double capacity = e.getAttribute(GraphConstant.ATTRIBUTE_EDGE_CAPACITY);
            double load = e.getAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD);
            double length = capacity / (Math.pow(capacity - load, 2));
            length = (1 / sigma) * length;  //每条边添加一个length属性
            e.addAttribute(GraphConstant.ATTRIBUTE_EDGE_LENGTH, length);
        }

        Node source = tmp.getNode(cfe.getNodeSource()); //获取当前flow的source节点

        // CSPF --> Dijkstra
        /*
         * 以length为权值生成Dijkstra对象
         * source为源
         * tmp为图
         */
        Dijkstra d = new Dijkstra(Dijkstra.Element.EDGE, "result", GraphConstant.ATTRIBUTE_EDGE_LENGTH);
        d.init(tmp);
        d.setSource(source);
        d.compute();

        Node dest = tmp.getNode(cfe.getNodeDestination());  //获取当前flow的dest节点

        int count = 0;
        for (Path ignored : d.getAllPaths(dest)) {    //计算从source到dest的路径数目
            count++;
        }

        Path path = cfe.getPath();  //获取当前flow先前生成的path
        double pathLength = 0.0;
        for (Edge e : path.getEdgeSet()) {  //求出该path上所有边的length之和
            pathLength += (double) tmp.getEdge(e.getId()).getAttribute(GraphConstant.ATTRIBUTE_EDGE_LENGTH);
        }

        if (count > 1) {    //如果从source到dest的路径大于1
            //获取最短length的path
            for (Path p : d.getAllPaths(dest)) {    //遍历从source到dest的路径
                double pl = 0.0;
                for (Edge e : p.getEdgeSet()) {     //求解当前路径的路径长度pl
                    pl += (double) tmp.getEdge(e.getId()).getAttribute(GraphConstant.ATTRIBUTE_EDGE_LENGTH);
                }

                if ((pl < pathLength) && (!pathEquals(p, path))) {  //如果pl小于pathLength
                    //更新path和pathLength
                    path = p;
                    pathLength = pl;
                }
            }

            // Sum discovered path to temporary graph
            // 将该最短路作为当前flow的路径
            for (Edge e : path.getEdgeSet()) {  //遍历该最短路上的边
                double load = tmp.getEdge(e.getId()).getAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD);   //获取当前边的负载
                load += cfe.getBandwidth(); //负载加上flow带宽
                tmp.getEdge(e.getId()).addAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD, load);   //更新tmp中的负载
            }

            // BEFORE re-calculate the delay time, re-add previously pruned edges to temporary graph
            // 将之前从tmp中删除的边包括其属性再添加回去
            for (Edge e : pruned) {
                Edge ne = tmp.addEdge(e.getId(), e.getSourceNode(), e.getTargetNode(), this.directedEdge);
                for (String keyAttribute : e.getAttributeKeySet()) {
                    ne.addAttribute(keyAttribute, e.getAttribute(keyAttribute));
                }
            }

            double localTglob = averageDelayCalculation(tmp);   //重新计算平均延迟
            if (localTglob < Tfin) {    //如果延迟变小了
                finalTimeAllocation(localTglob);    //则更新Tfin

                // Delete OLD path for the current flow from the graph
                // 从inner中将原来该flow的路径删除
                for (Edge e : cfe.getPath().getEdgeSet()) {
                    double load = inner.getEdge(e.getId()).getAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD);
                    load -= cfe.getBandwidth();
                    inner.getEdge(e.getId()).addAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD, load);
                }

                // Add NEW path for the current flow to the graph
                // 将新求解出来的path作为当前flow的路径添加到inner中
                for (Edge e : path.getEdgeSet()) {
                    double load = inner.getEdge(e.getId()).getAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD);
                    load += cfe.getBandwidth();
                    inner.getEdge(e.getId()).addAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD, load);
                }

                // Add NEW path to the current flow
                cfe.setPath(path);  //当前flow更新path
            }
        } else {    //如果从source到dest的路径只有一条
            Path found = d.getPath(dest);   //获取该路径found
            double dijkstraPathLength = 0.0;
            for (Edge e : found.getEdgeSet()) { //救出该路径的总length
                dijkstraPathLength += (double) e.getAttribute(GraphConstant.ATTRIBUTE_EDGE_LENGTH);
            }

            if (dijkstraPathLength < pathLength) {  //如果该路径比先前生成的pathLength小
                // Sum discovered path to temporary graph
                for (Edge e : found.getEdgeSet()) { //则将found作为当前flow的路径
                    double load = tmp.getEdge(e.getId()).getAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD);
                    load += cfe.getBandwidth();
                    tmp.getEdge(e.getId()).addAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD, load);   //更新tmp中的负载
                }

                // BEFORE re-calculate the delay time, re-add previously pruned edges to temporary graph
                // 将之前删除的边包括属性添加回tmp
                for (Edge e : pruned) {
                    Edge ne = tmp.addEdge(e.getId(), e.getSourceNode(), e.getTargetNode(), this.directedEdge);
                    for (String keyAttribute : e.getAttributeKeySet()) {
                        ne.addAttribute(keyAttribute, e.getAttribute(keyAttribute));
                    }
                }

                double localTglob = averageDelayCalculation(tmp);   //计算平均延迟
                if (localTglob < Tfin) {    //如果延迟变小了
                    finalTimeAllocation(localTglob);    //则更新Tfin

                    // Delete OLD path for the current flow from the graph
                    // 将旧的path从inner中删除
                    for (Edge e : path.getEdgeSet()) {
                        double load = inner.getEdge(e.getId()).getAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD);
                        load -= cfe.getBandwidth();
                        inner.getEdge(e.getId()).addAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD, load);
                    }

                    // Add NEW path for the current flow to the graph
                    // 将新生成的found路径加入到inner中
                    for (Edge e : found.getEdgeSet()) {
                        double load = inner.getEdge(e.getId()).getAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD);
                        load += cfe.getBandwidth();
                        inner.getEdge(e.getId()).addAttribute(GraphConstant.ATTRIBUTE_EDGE_LOAD, load);
                    }

                    // Add NEW path to the current flow
                    cfe.setPath(found); //将found作为当前flow的路径
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
        if (Tfin < Tglob) { //如果平均延迟变小了
            Tglob = Tfin;   //则更新Tglob
            return true;    //继续迭代
        } else return !(Tfin == Tglob);

    }


    /**
     * Check whether path1 == path2
     *
     * @param p1
     * @param p2
     * @return True or False
     */
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

    /**
     * Copy Graph
     *
     * @param graph
     * @param name
     * @return Graph
     */
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
