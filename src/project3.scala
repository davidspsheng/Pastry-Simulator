import akka.actor._
import akka.routing.RoundRobinRouter
import scala.collection.mutable.Map
import java.util.HashMap

object project3 {

  val maxVal: Long = math.pow(2, 32).toLong
  var nodeID = Array[NodeId]()
  var numPass: Int = 0
  var listPass = Array[Int]()
  
  def main(args: Array[String]){
	  var numNodes: Int = 10000
	  var numRequests: Int = 10
	  
	  nodeID = new Array[NodeId](numNodes + 1)
	  for(i <- 1 to numNodes){
	    nodeID(i) = new NodeId(i, 0, "0", null)
	  }
	  listPass = new Array[Int](numNodes + 1)
	  for(i <- 1 to numNodes){
	    listPass(i) = 0
	  }
	  
	  start(numNodes, numRequests)
  }
  
  case class Start
  case class Register(i: Int)
  case class AddSecondNode(i: Int)
  case class FinishAddNodes
  case class FinishFeedBackNewNode(i: Int)
  case class FinishRouting(msg: Int, key: Int, lastNode: Int)
  
  case class GiveIndex(i: Int, numNodes: Int)
  case class SelfJoin(i: Int)
  case class JoinRouter(key: Int, lSet: Array[Int], nSet: Array[Int], stateTable: Array[Int], flag: Int)
  case class UpdateNewNode(key: Int, lSet: Array[Int], nSet: Array[Int], stateTable: Array[Int])
  case class FeedBack(s: Array[Int], i: Int)
  case class Route(msg: Int, key: Int, queue: Array[Int])
  case class StartRouting(numNodes: Int, numRequests: Int)
  
  class Boss(numNodes: Int, numRequests: Int) extends Actor{
    val hex: Int = 16
    val len: Int = 8
    var numNodesRegistered: Int = 0;
    var numJoin: Int = 0
    var numReqCol: Int = 0
    var totalMSG: Int = 0
    
    val nodesRouter = context.actorOf(Props[Node].withRouter(RoundRobinRouter(numNodes)), name = "nodesRouter")
    
    def to16(n: Long) :String = {
    	var str: String = ""
        var t: Long = n
        var i: Int = len - 1
        while(i >= 0){
          var k: Int = (t / math.pow(hex, i).toLong).toInt
          var s:String = ""
          if(k >= 10){
            if(k == 10){s = "a"}
            if(k == 11){s = "b"}
            if(k == 12){s = "c"}
            if(k == 13){s = "d"}
            if(k == 14){s = "e"}
            if(k == 15){s = "f"}
          }
          else{s = String.valueOf(k)}
          str += s
          t = t - (k * math.pow(hex, i)).toLong
          
          i -= 1
        }
    	return str
  }
    
    def AddNode(i: Int){
      // 1--(i-1) node have be added
	  	var id: Long = (maxVal * java.lang.Math.random()).toLong
		var s: String = to16(id)
		nodeID(i).nodeIdIn = id
		nodeID(i).nodeIdString = s
		if(i == 1){
		  nodeID(i).actroRef ! SelfJoin(i)
		}
		else{
		  var j: Int = ((i - 1) * java.lang.Math.random()).toInt + 1
		  numPass += 1
		  listPass(numPass) = j
		  nodeID(j).actroRef ! JoinRouter(i, null, null, null, 0)
		}
    }
    
    def AssignReqs(){
      for(i <- 1 to numNodes){
        nodeID(i).actroRef ! StartRouting(numNodes, numRequests)
      }
    }
    
    def receive = {
      case Start =>
        numNodesRegistered = 0
        
        for(i <- 1 to numNodes){
          nodesRouter ! GiveIndex(i, numNodes)
        }
        
      case Register(i) =>
        nodeID(i).actroRef = sender
        this.numNodesRegistered += 1
        if(this.numNodesRegistered == numNodes){
        	AddNode(1)
        }
        
      case FinishAddNodes =>
        this.AssignReqs()
        
      case AddSecondNode(i) =>
        AddNode(i + 1)
               
      case FinishFeedBackNewNode(i) =>
        numPass -= 1
        if(numPass == 0){
          if(i == numNodes)
        	  self ! FinishAddNodes
          else
        	  AddNode(i + 1)
        }
        
      case FinishRouting(msg: Int, key: Int, lastNode: Int) =>
        this.totalMSG += msg
        this.numReqCol += 1
        println(this.numReqCol + "/" + numNodes*numRequests + "---(" + msg + "," + key + "," + lastNode + ")----arrives!")
        if(this.numReqCol == numNodes * numRequests){
          var avg: Double = this.totalMSG.toDouble / (numNodes * numRequests).toDouble
          var std: Double = Math.log(numNodes) / Math.log(16)
          println("The average hops is " + avg + "/" + std)
          context.system.shutdown()
        }
        
      case _ =>
        println("Unknown Boss Message!")
    }
  }
  
  class Node extends Actor{
    val hex: Int = 16
    val len: Int = 8
    val d: Int = 8
    var nodeId: NodeId = new NodeId(0, 0, "", null)
    var bossRef: ActorRef = null
    var leafSet = Array[Int]()
    var routingTable = Array.ofDim[Int](len, hex)
    var neighborSet = Array[Int]()
    
    def route(msg: Int, key: Int, queue: Array[Int]){
        var j: Int = 0
        var s: Int = this.sLeafSet()
        var ll: Int = this.lLeafSet()
        if(s != 0 && ll != 0 && nodeID(this.leafSet(s)).nodeIdIn <= nodeID(key).nodeIdIn && nodeID(key).nodeIdIn <= nodeID(this.leafSet(ll)).nodeIdIn){
          //	key is within range of our leaf set
          var t: Long = -1
          for(i <- 1 to d){
            if(this.leafSet(i) != 0){
	            if(t == -1){
	              t = java.lang.Math.abs(nodeID(this.leafSet(i)).nodeIdIn - nodeID(key).nodeIdIn)
	              j = this.leafSet(i)
	            }
	            else{
	              if(t > java.lang.Math.abs(nodeID(this.leafSet(i)).nodeIdIn - nodeID(key).nodeIdIn)){
	                t = java.lang.Math.abs(nodeID(this.leafSet(i)).nodeIdIn - nodeID(key).nodeIdIn)
	                j = this.leafSet(i)
	              }
	            }
            }
          }
        }
        else{
          //	use the routing table
          var l: Int = shl(key, this.nodeId.i)
          var c: Char = nodeID(key).nodeIdString.charAt(l)
          var col: Int = 0
          if(c == 'a'){col = 10}
          else if(c == 'b'){col = 11}
          else if(c == 'c'){col = 12}
          else if(c == 'd'){col = 13}
          else if(c == 'e'){col = 14}
          else if(c == 'f'){col = 15}
          else{col = Integer.parseInt(c.toString())}
          
          if(this.routingTable(l)(col) != 0){
            j = this.routingTable(l)(col)
          }
          else{
            //	rare case
            var l: Int = shl(key, this.nodeId.i)
        	  // leaf part
            for(i <- 1 to this.d){
              if(this.leafSet(i) != 0){
	              var T: Long = java.lang.Math.abs(nodeID(this.leafSet(i)).nodeIdIn - nodeID(key).nodeIdIn)
	              var A: Long = java.lang.Math.abs(nodeID(this.nodeId.i).nodeIdIn - nodeID(key).nodeIdIn)
	              if(shl(this.leafSet(i), key) >= l && T < A){
	                if(j == 0)
	                  j = this.leafSet(i)
	              }
              }
            }
            	// routing table part
            for(i <- 0 to (len - 1))
              for(k <- 0 to (hex - 1)){
                if(this.routingTable(i)(k) != 0){
	                var T: Long = java.lang.Math.abs(nodeID(this.routingTable(i)(k)).nodeIdIn - nodeID(key).nodeIdIn)
	                var A: Long = java.lang.Math.abs(nodeID(this.nodeId.i).nodeIdIn - nodeID(key).nodeIdIn)
	                if(shl(this.routingTable(i)(k), key) >= l && T < A){
	                	if(j == 0)
	                		j = this.routingTable(i)(k)
	                }
                }
              }
            
            	// neighbor part
            for(i <- 1 to this.d){
              if(this.neighborSet(i) != 0){
	              var T: Long = java.lang.Math.abs(nodeID(this.neighborSet(i)).nodeIdIn - nodeID(key).nodeIdIn)
	              var A: Long = java.lang.Math.abs(nodeID(this.nodeId.i).nodeIdIn - nodeID(key).nodeIdIn)
	              if(shl(this.neighborSet(i), key) >= l && T < A){
	                if(j == 0)
	                  j = this.neighborSet(i)
	              }
              }
            }
          }
        }
        if(j == 0 || key == this.nodeId.i){
        	// this is the last node on the routing path
          this.deliver(msg, key)
        }
        else{
        	// continue routing
          var flagg: Int = 0
          for(jj <- 1 to (queue.length - 1)){
            if(queue(jj) == j)
              flagg = 1
          }
          if(flagg == 0){	//	j has already existed in the queue, this node is numerically closest to key
	          this.forward(msg, key, j, queue)
          }
          else{
            this.deliver(msg, key)
          }
        }
    }
    
    def deliver(msg: Int, key: Int){	//	when the message is received, report to boss
//      numPass = 0
      bossRef ! FinishRouting(msg, key, this.nodeId.i)
    }
    
    def forward(msg: Int, key: Int, nextId: Int, queue: Array[Int]){
      nodeID(nextId).actroRef ! Route(msg, key, queue)
    }
    
    def newLeafs(leavesSet: Array[Int]){
      for(i <- 1 to (leavesSet.length - 1)){
        if(leavesSet(i) != 0){
          var flag: Int = 0
          for(j <- 1 to this.d){
	          if(leavesSet(i) == this.leafSet(j)){
	            flag = 1
	          }
          }
          if(flag == 0){
            changeLeaf(leavesSet(i))
          }
        }
      }
    }
    
    def changeLeaf(n: Int){
      if(nodeID(n).nodeIdIn < nodeID(this.nodeId.i).nodeIdIn){
        var i: Int = this.d / 2
        while(i >= 1){
          if(this.leafSet(i) != 0){
	          var Tn: Long = nodeID(this.nodeId.i).nodeIdIn - nodeID(n).nodeIdIn
	          var Tl :Long = nodeID(this.nodeId.i).nodeIdIn - nodeID(this.leafSet(i)).nodeIdIn
	          if(Tn < Tl){
	            for(j <- 1 to i){
	              this.leafSet(j) = this.leafSet(j + 1)
	            }
	            this.leafSet(i) = n
	            i = 0
	          }
          }
          else{
            this.leafSet(i) = n
            i = 0
          }
          
          i -= 1
        }
      }
      if(nodeID(n).nodeIdIn > nodeID(this.nodeId.i).nodeIdIn){
        var i: Int = this.d / 2
        while(i <= this.d){
          if(this.leafSet(i) != 0){
	          var Tn: Long = nodeID(n).nodeIdIn - nodeID(this.nodeId.i).nodeIdIn
	          var Tl :Long = nodeID(this.leafSet(i)).nodeIdIn - nodeID(this.nodeId.i).nodeIdIn
	          if(Tn < Tl){
	            var j: Int = this.d
	            while(j >= i){
	              this.leafSet(j) = this.leafSet(j - 1)
	              j -= 1
	            }
	            this.leafSet(i) = n
	            i = this.d
	          }
          }
          else{
            this.leafSet(i) = n
            i = this.d
          }
          
          i += 1
        }
      }
    }
    
    def shl(d: Int, a: Int): Int = {
      var k: Int = 0
      for(i <- 0 to (len - 1)){
        if(nodeID(d).nodeIdString.charAt(i) != nodeID(a).nodeIdString.charAt(i)){
          k = i
          return k
        }
      }
      return (len - 1)
    }
    
    def sLeafSet(): Int = {
      var k: Int = 0
      for(i <- 1 to this.d/2){
        if(this.leafSet(i) != 0)
          return i
      }
      
      return k
    }
    
    def lLeafSet(): Int = {
      var k: Int = 0
      var i: Int = this.d
      while(i > this.d/2){
        if(this.leafSet(i) != 0)
          return i
        i -= 1
      }
      
      return k
    }
    
    def GetEntryIndex(s: Char): Int = {
      var k: Int = 0
      if(s == 'a'){k = 10}
      else if(s == 'b'){k = 11}
      else if(s == 'c'){k = 12}
      else if(s == 'd'){k = 13}
      else if(s == 'e'){k = 14}
      else if(s == 'f'){k = 15}
      else{k = Integer.parseInt(s.toString())}
      
      return k
    }
    
    def print(s1: String){
      var s: String = s1 + "\n"
      s += "This node is " + this.nodeId.i
      s += "\nLeafSet:\n"
      for(i <- 1 to this.d){
        if(this.leafSet(i) != 0){
        	s += "(" + nodeID(this.leafSet(i)).i + "," + nodeID(this.leafSet(i)).nodeIdString + "," + nodeID(this.leafSet(i)).nodeIdIn + ")"
        }
        else{
          s += "(0,0,0)"
        }
          
      }
      s += "\n"
      s += "RoutingTable:\n"
      for(i <- 0 to (this.len - 1)){
        for(j <- 0 to (this.hex - 1)){
          if(this.routingTable(i)(j) == 0){
            s += "(0,0,0)"
          }
          else if(this.routingTable(i)(j) == -1){
            s += "(-1,-1,-1)"
          }
          else{
            s += "(" + nodeID(this.routingTable(i)(j)).i + "," + nodeID(this.routingTable(i)(j)).nodeIdString + "," + nodeID(this.routingTable(i)(j)).nodeIdIn + ")"
          }
        }
        s += "\n"
      }
      s += "NeighborSet:\n"
      for(i <- 1 to this.d){
        if(this.neighborSet(i) != 0){
        	s += "(" + nodeID(this.neighborSet(i)).i + "," + nodeID(this.neighborSet(i)).nodeIdString + "," + nodeID(this.neighborSet(i)).nodeIdIn + ")"
        }
        else{
          s += "(0,0,0)"
        }
      }
      s += "\n"
      s = ""
      if(s1.equals("UpdateNewNode:")){
        s += "UpdateNewNode: " + this.nodeId.i + "\n"
        s += "FeedBack Queue:\n"
        for(i <- 1 to numPass){
          s += listPass(i) + ","
        }
        s += "\n"
      }
      else{
         s += "FeedBack " + this.nodeId.i + "\n"
      }
      println(s)
    }
    
    def receive = {
      case GiveIndex(i, numNodes) =>
        	//	initial part
        this.nodeId.i = i
        this.nodeId.actroRef = self
        leafSet = new Array[Int](this.d + 1)
        neighborSet = new Array[Int](this.d + 1)
        for(i <- 1 to this.d){
          leafSet(i) = 0
          neighborSet(i) = 0
        }
        for(i <- 0 to (len - 1)){
          for(j <- 0 to (hex - 1))
            routingTable(i)(j) = 0
        }
        this.bossRef = sender
        	// end initial part
        sender ! Register(i)
      
      case SelfJoin(i) =>
        this.nodeId.nodeIdIn = nodeID(i).nodeIdIn
        this.nodeId.nodeIdString = nodeID(i).nodeIdString
        bossRef ! AddSecondNode(i)
        
      case JoinRouter(key, lSet, nSet, stateTable, flag) =>
        var j: Int = 0
        var s: Int = this.sLeafSet()
        var ll: Int = this.lLeafSet()
        if(s != 0 && ll != 0 && nodeID(this.leafSet(s)).nodeIdIn <= nodeID(key).nodeIdIn && nodeID(key).nodeIdIn <= nodeID(this.leafSet(ll)).nodeIdIn){
          //	key is within range of our leaf set
          var t: Long = -1
          for(i <- 1 to d){
            if(this.leafSet(i) != 0){
	            if(t == -1){
	              t = java.lang.Math.abs(nodeID(this.leafSet(i)).nodeIdIn - nodeID(key).nodeIdIn)
	              j = this.leafSet(i)
	            }
	            else{
	              if(t > java.lang.Math.abs(nodeID(this.leafSet(i)).nodeIdIn - nodeID(key).nodeIdIn)){
	                t = java.lang.Math.abs(nodeID(this.leafSet(i)).nodeIdIn - nodeID(key).nodeIdIn)
	                j = this.leafSet(i)
	              }
	            }
            }
          }
        }
        else{
          //	use the routing table
          var l: Int = shl(key, this.nodeId.i)
          var c: Char = nodeID(key).nodeIdString.charAt(l)
          var col: Int = 0
          if(c == 'a'){col = 10}
          else if(c == 'b'){col = 11}
          else if(c == 'c'){col = 12}
          else if(c == 'd'){col = 13}
          else if(c == 'e'){col = 14}
          else if(c == 'f'){col = 15}
          else{col = Integer.parseInt(c.toString())}
          
          if(this.routingTable(l)(col) != 0){
            j = this.routingTable(l)(col)
          }
          else{
            //	rare case
            var l: Int = shl(key, this.nodeId.i)
        	  // leaf part
            for(i <- 1 to this.d){
              if(this.leafSet(i) != 0){
	              var T: Long = java.lang.Math.abs(nodeID(this.leafSet(i)).nodeIdIn - nodeID(key).nodeIdIn)
	              var A: Long = java.lang.Math.abs(nodeID(this.nodeId.i).nodeIdIn - nodeID(key).nodeIdIn)
	              if(shl(this.leafSet(i), key) >= l && T < A){
	                if(j == 0)
	                  j = this.leafSet(i)
	              }
              }
            }
            	// routing table part
            for(i <- 0 to (len - 1))
              for(k <- 0 to (hex - 1)){
                if(this.routingTable(i)(k) != 0){
	                var T: Long = java.lang.Math.abs(nodeID(this.routingTable(i)(k)).nodeIdIn - nodeID(key).nodeIdIn)
	                var A: Long = java.lang.Math.abs(nodeID(this.nodeId.i).nodeIdIn - nodeID(key).nodeIdIn)
	                if(shl(this.routingTable(i)(k), key) >= l && T < A){
	                	if(j == 0)
	                		j = this.routingTable(i)(k)
	                }
                }
              }
            
            	// neighbor part
            for(i <- 1 to this.d){
              if(this.neighborSet(i) != 0){
	              var T: Long = java.lang.Math.abs(nodeID(this.neighborSet(i)).nodeIdIn - nodeID(key).nodeIdIn)
	              var A: Long = java.lang.Math.abs(nodeID(this.nodeId.i).nodeIdIn - nodeID(key).nodeIdIn)
	              if(shl(this.neighborSet(i), key) >= l && T < A){
	                if(j == 0)
	                  j = this.neighborSet(i)
	              }
              }
            }
          }
        }
//        var l = Array[Int]()	// leaf Set
        var r = Array[Int]()	//	Routing Table
        var n = Array[Int]()	//	Neighbor Set
        
        if(flag == 0){
          n = this.neighborSet
        }
        else{
          n = nSet
        }
        // merge routing table part
        var numTRouTa: Int = 0
        var tRouTa = new Array[Int](d + hex * len + d + 1)    
        for(i <- 1 to this.d){
          if(this.leafSet(i) != 0){
            numTRouTa += 1
            tRouTa(numTRouTa) = this.leafSet(i)
          }
        }
        for(i <- 0 to (len - 1))
          for(k <- 0 to (hex - 1)){
            if(this.routingTable(i)(k) != 0){
              numTRouTa += 1
              tRouTa(numTRouTa) = this.routingTable(i)(k)
            }
          }
        for(i <- 1 to this.d){
          if(this.neighborSet(i) != 0){
            numTRouTa += 1
            tRouTa(numTRouTa) = this.neighborSet(i)
          }
        }
        var stateTablelen: Int = 0
        if(stateTable == null){
          stateTablelen = 0
        }
        else{
          stateTablelen = stateTable.length
        }
        r = new Array[Int](stateTablelen + numTRouTa + 1)
        for(i <- 1 to (stateTablelen - 1))
          r(i) = stateTable(i)
        for(i <- 1 to numTRouTa)
          r(i + stateTablelen - 1) = tRouTa(i)
        r(stateTablelen + numTRouTa) = this.nodeId.i
        if(j == 0){
        	// this is the last node on the routing path
          nodeID(key).actroRef ! UpdateNewNode(key, this.leafSet, n, r)
        }
        else{
        	// continue routing
          var flagg: Int = 0
          for(jj <- 1 to numPass){
            if(listPass(jj) == j)
              flagg = 1
          }
          if(flagg == 0){	//	j has already existed in the queue, this node is numerically closest to key
	          numPass += 1
	          listPass(numPass) = j
	          nodeID(j).actroRef ! JoinRouter(key, null, n, r, 1)
          }
          else{
            nodeID(key).actroRef ! UpdateNewNode(key, this.leafSet, n, r)
          }
        }
        
      case UpdateNewNode(key, lSet, nSet, stateTable) =>
        this.nodeId.nodeIdIn = nodeID(key).nodeIdIn
        this.nodeId.nodeIdString = nodeID(key).nodeIdString
        this.leafSet = lSet
        this.neighborSet = nSet
        for(i <- 1 to (stateTable.length - 1)){
          var row: Int = shl(key, stateTable(i))
          var col: Int = GetEntryIndex(nodeID(stateTable(i)).nodeIdString.charAt(row))
          if(this.routingTable(row)(col) == 0){
            this.routingTable(row)(col) = stateTable(i)
          }
        }
        ///
        this.print("UpdateNewNode:")
        ///
        var s = new Array[Int](this.d + this.len * this.hex + this.d + 1 + 1)
        for(i <- 1 to this.d)
          s(i) = this.leafSet(i)
        var k: Int = 1
        for(i <- 0 to (this.len - 1)){
          for(j <- 0 to (this.hex - 1)){
            s(this.d + k) = this.routingTable(i)(j)
            k += 1
          }
        }
        for(i <- 1 to this.d)
          s(this.d + this.len * this.hex + i) = this.neighborSet(i)
        s(this.d + this.len * this.hex + this.d + 1) = this.nodeId.i
        for(i <- 1 to numPass){
          nodeID(listPass(i)).actroRef ! FeedBack(s, key)
        }
        
      case FeedBack(s, i) =>
        	// update leaf Set
        this.newLeafs(s)
        	// update routing table
        for(i <- 0 to (s.length - 1)){
          if(s(i) != 0){
	          var row: Int = shl(this.nodeId.i, s(i))
	          var col: Int = GetEntryIndex(nodeID(s(i)).nodeIdString.charAt(row))
	          if(this.routingTable(row)(col) == 0){
	            this.routingTable(row)(col) = s(i)
	          }
          }
        }
        	// update neighbor Set
        for(i <- 1 to this.d){
          var k: Int = 0
          if(this.neighborSet(i) == 0){
            if(k == 0){
            	k = ((s.length - 1) * java.lang.Math.random()).toInt + 1
            }
            else{
              k = (k + 1) % (s.length - 1)
              if(k == 0){
                k += 1
              }
            }
            
            this.neighborSet(i) = s(k)
          }
        }
        
        this.print("FeedBack: ")
        bossRef ! FinishFeedBackNewNode(i)
        
      case StartRouting(numNodes: Int, numRequests: Int) =>
        for(i <- 1 to numRequests){
          var key: Int = (numNodes * java.lang.Math.random()).toInt + 1
          var queue = new Array[Int](1)
          queue(0) = 0
          self ! Route(0, key, queue)
          
          Thread.sleep(1)
//          context.system.scheduler
        }
        
        
      case Route(msg: Int, key: Int, queue: Array[Int]) =>
      	var q = new Array[Int](queue.length + 1)
      	for(i <- 1 to (queue.length - 1)){
      	  q(i) = queue(i)
      	}
      	q(queue.length) = this.nodeId.i
        this.route(msg + 1, key, q)
        
      case _ =>
        println("Unknown Node Message!")
    }
  }
  
  
  def start(numNodes: Int, numRequests: Int){
	  val system = ActorSystem("project3System")
	  
	  val boss = system.actorOf(Props(new Boss(numNodes, numRequests)), name = "boss")
	  
	  boss ! Start
  }
  
}

class NodeId(var ii: Int, var nIdIn: Int, var nId: String, var aRef: ActorRef) {
	var i: Int = ii
	var nodeIdIn: Long = nIdIn
	var nodeIdString: String = nId
	var actroRef: ActorRef = aRef
	
	def print() {
	  println(this.i + " " + this.nodeIdIn + " " + this.nodeIdString + " " + this.actroRef)
	}
}