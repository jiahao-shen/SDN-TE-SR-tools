package it.unipr.netsec.sdn.run;

/*
 *
 * @author Luca Davoli - <a href="mailto:lucadavo@gmail.com">lucadavo@gmail.com</a> - Department of Information Engineering - University of Parma
 *
 */

import it.unipr.netsec.sdn.algorithm.FlowAssignmentAlgorithm;
import it.unipr.netsec.sdn.benchmark.BenchmarkFactory;
import it.unipr.netsec.sdn.graph.GraphFactory;
import it.unipr.netsec.sdn.segmentrouting.SegmentRouting;
import it.unipr.netsec.sdn.segmentrouting.SegmentRoutingCatalogue;
import it.unipr.netsec.sdn.trafficflow.TrafficFlowFactory;
import it.unipr.netsec.sdn.trafficflow.element.FlowElement;
import it.unipr.netsec.sdn.trafficflow.element.TrafficFlowContainer;
import it.unipr.netsec.sdn.util.CMDParams;
import it.unipr.netsec.sdn.util.Utils;
import org.graphstream.graph.Edge;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.graph.Path;

import java.util.Arrays;

public class Main {

    public static void main(String[] args) {
        //获取运行参数
        CMDParams params = Utils.parseCMD(args);
        String topo = params.get(Utils.CMDPARAMS_TOPO_IN);    //获取links.json文件路径
        String outTopo = params.get(Utils.CMDPARAMS_TOPO_OUT);    //获取out_links.json文件路径

        String flows = params.get(Utils.CMDPARAMS_FLOWS_IN);    //获取flow_catalogue.json文件路径
        String outFlows = params.get(Utils.CMDPARAMS_FLOWS_OUT);    //获取out_flow_catalogue.json文件路径

        boolean DEBUG = Boolean.parseBoolean(params.get(Utils.CMDPARAMS_DEBUG));    //是否开启debug模式

        //拓扑图构建
        GraphFactory gf = new GraphFactory();
        gf.loadTopologyCatalogueFromJSONFile(topo); //从links.json文件加载Graph
        gf.buildGraphStreamTopology();
//        GraphFactory.displayPoorGraph(gf.getGraph(), false);    //绘制Topology

        //Traffic Flow构建
        TrafficFlowFactory tff = new TrafficFlowFactory();
        tff.loadFlowCatalogueFromJSONFile(flows);   //从flow_catalogue.json加载Traffic Flow
        if (DEBUG) {
            System.out.println("Flows #: " + tff.getTrafficFlows().size());
        }

        long deltaFlowAssignment;   //流分配计算时间
        long deltaSegmentRouting;   //SegmentRouting计算时间

        //流量分配算法
        FlowAssignmentAlgorithm faa = new FlowAssignmentAlgorithm();    //流量分配算法
        faa.init(gf.getGraph());    //初始化Topology
        faa.setTrafficFlows(tff.getTrafficFlows()); //设置Traffic Flow
        deltaFlowAssignment = System.currentTimeMillis();
        faa.compute();  //计算
        deltaFlowAssignment = System.currentTimeMillis() - deltaFlowAssignment;
        faa.terminate();    //结束

        Graph finalGraph = faa.getUpdatedGraph();   //分配后的拓扑图
        TrafficFlowContainer finalTrafficFlowAssignment = faa.getFlowAssignment();  //获取流量分配

        GraphFactory.displayGraphWithFlows(finalGraph, finalTrafficFlowAssignment, false);  //显示分配后的拓扑图

        //如果是Debug模式则输出对应的流分配信息
        if (DEBUG) {
            System.out.println();
            System.out.println("FLOW ALLOCATION");
            System.out.println("\tID\tFlow\t\tPath");
            for (FlowElement fe : finalTrafficFlowAssignment) {
                System.out.print("\t" + fe.getId());
                System.out.print("\t(" + fe.getNodeSource() + "," + fe.getNodeDestination() + ")");
                System.out.print("\t\t");
                String path = "";
                for (Edge e : fe.getPath().getEdgeSet()) {
                    path += "(" + e.getSourceNode() + "," + e.getTargetNode() + "),";
                }
                if (path.length() > 0) {
                    System.out.print(path.substring(0, path.length() - 1));
                } else {
                    System.out.println("NO PATH!");
                }
                System.out.println();
            }
            System.out.println();
        }

        //Segments计算
        if (DEBUG) {
            System.out.println();
            System.out.println("SEGMENT ROUTING ALLOCATION");
        }

        //对每条流生成对应的Segments
        SegmentRoutingCatalogue srCatalogue = new SegmentRoutingCatalogue();    //段路由策略
        deltaSegmentRouting = System.currentTimeMillis();
        for (FlowElement fe : finalTrafficFlowAssignment) { //遍历每一个Flow
            Path assignedPath = fe.getPath();   //获取分配路径
            Path naturalPath = SegmentRouting.getNaturalPath(gf.getGraph(), fe.getNodeSource(), fe.getNodeDestination());   //获取一条等价的从source到dest的路径
            try {
                Node[] segments = SegmentRouting.getSegments(gf.getGraph(), assignedPath);  //获取Segments
                srCatalogue.addSegmentRoutingElement(fe, naturalPath, segments);    //添加段路由元素
                tff.addSegmentsToTrafficFlow(fe, segments); //给当前flow添加segments
            } catch (Exception e) {
                e.printStackTrace();
                srCatalogue.addSegmentRoutingElement(fe, naturalPath, new Node[0]);
                tff.addSegmentsToTrafficFlow(fe, new Node[0]);
            }
        }
        deltaSegmentRouting = System.currentTimeMillis() - deltaSegmentRouting;

        //打印每个Flow对应的Assigned Path,Natural Path and Segments
        if (DEBUG) {
            int countFlows = 0;
            for (FlowElement fe : srCatalogue.getFlowElements()) {
                System.out.println("\tFlow: (" + fe.getNodeSource() + "," + fe.getNodeDestination() + "): " + fe.getId());

                Path assignedPath = srCatalogue.getAssignedPath(fe.getId());
                Path naturalPath = srCatalogue.getNaturalPath(fe.getId());

                System.out.println("\t\tAssigned path:\t" + assignedPath + " --> Len = " + assignedPath.size());
                System.out.println("\t\tNatural path:\t" + naturalPath + " --> Len = " + naturalPath.size());

                try {
                    Node[] segments = srCatalogue.getSegments(fe.getId());
                    System.out.println("\t\tSegments:\t" + Arrays.toString(segments) + " --> Len = " + segments.length);
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                }
                System.out.println();
                countFlows++;
            }
            System.out.println("Flows #: " + countFlows);
        }

        //输出拓扑保存到JSON文件中
        gf.saveTopologyToTopologyCatalogue(finalGraph, srCatalogue, outTopo);

        //输出Traffic Flow到JSON文件中
        tff.saveTrafficFlowToFlowCatalogue(srCatalogue, outFlows);

        //输出算法所花的时间
        if (DEBUG) {
            System.out.println("FlowAssignment Time: " + deltaFlowAssignment + "ms");
            System.out.println("SegmentRouting Time: " + deltaSegmentRouting + "ms");

            //BenchmarkFactory.histogram(srCatalogue, Integer.toString(gf.getGraph().getNodeCount()), Integer.toString(tff.getTrafficFlows().size()));
            //BenchmarkFactory.average(srCatalogue);
        }
    }

}
