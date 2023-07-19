package rs.etf.sab.student;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class GraphUtilities {

	public static class Transition {
		int destination;
		int distance;

		public Transition(int destination, int distance) {
			super();
			this.destination = destination;
			this.distance = distance;
		}

		public int getDestination() {
			return destination;
		}

		public void setDestination(int destination) {
			this.destination = destination;
		}

		public int getDistance() {
			return distance;
		}

		public void setDistance(int distance) {
			this.distance = distance;
		}

	}

	private static final int INF = Integer.MAX_VALUE;

	private int[][] distances;
	private int[][] previousNodes;
	private int destination;
	private Set<Integer> shopLocations;
	private Set<Integer> shopsForOrder;

	public GraphUtilities(int[][] graph, int destination, Set<Integer> shopLocations, Set<Integer> shopsForOrder) {
		this.destination = destination;
		this.shopLocations = shopLocations;
		this.shopsForOrder = shopsForOrder;

		this.distances = new int[graph.length][graph.length];
		this.previousNodes = new int[graph.length][graph.length];

		for (int i = 0; i < distances.length; i++) {
			for (int j = 0; j < distances.length; j++) {
				distances[i][j] = graph[i][j];
				if (graph[i][j] != INF && i != j) {
					previousNodes[i][j] = i;
				} else {
					previousNodes[i][j] = -1;
				}
			}
		}
	}

	public void floydWarshall() {
		for (int k = 0; k < distances.length; k++) {
			for (int i = 0; i < distances.length; i++) {
				for (int j = 0; j < distances.length; j++) {
					if (distances[i][k] != INF && distances[k][j] != INF
							&& distances[i][k] + distances[k][j] < distances[i][j]) {
						distances[i][j] = distances[i][k] + distances[k][j];
						previousNodes[i][j] = previousNodes[k][j];
					}
				}
			}
		}
	}

	public int getClosestShop() {
		int minCityId = -1;
		int minDistance = INF;
		for (int i = 0; i < distances.length; i++) {
			if (minDistance > distances[destination][i] && shopLocations.contains(i)) {
				minCityId = i;
				minDistance = distances[destination][i];
			}
		}
		return minCityId;
	}

	private int calcTimeToClosestShop() {
		int tempDest = getClosestShop();
		if (tempDest == -1)
			return tempDest;
		int maxDistance = 0;
		for (int i = 0; i < distances.length; i++) {
			if (maxDistance < distances[tempDest][i] && shopsForOrder.contains(i))
				maxDistance = distances[tempDest][i];
		}
		return maxDistance;
	}

	public List<Transition> getTransitionsToBuyer() {
		int src = getClosestShop();
		int timeToClosestShop = calcTimeToClosestShop();
		List<Transition> path = buildPath(src, destination);
		for (int i = 0; i < path.size(); i++)
			path.get(i).setDistance(timeToClosestShop + path.get(i).getDistance());
		return path;
	}

	private List<Transition> buildPath(int src, int dst) {
		if (src == dst) {
			return new ArrayList<Transition>();
		} else if (previousNodes[src][dst] == -1) {
			return null;
		} else {
			List<Transition> retval = buildPath(src, previousNodes[src][dst]);
			retval.add(new Transition(dst, distances[src][dst]));
			return retval;
		}
	}
}
