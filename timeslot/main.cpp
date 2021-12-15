#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#define DEBUG 1

struct Route {
    int id;
    int startLaneId;
    int endLaneId;
    int length;
};
struct Lane {
    int id;
    int totalCost;
    int outDegree;
    int averageCost;
    bool remove;
    int unhandledRequest;
    std::vector<Route> routes;
};
struct Intersection {
    std::string id;
    int numberOfLane;
    std::vector<Lane> lanes;
    std::vector<Route> routes;
};
struct Request {
    std::string roadsiteId;
    int carId;
    int carLength;
    int carSpeed;
    int frontCarId;
    int distanceBetweenFrontCar;
    int routeId;
    int laneId;
    int startTime;
    int endTime;
};
struct Response {
    std::string routeId;
    std::string carId;
    enum Status { accepted, rejected, modified };
    Status status;
    int startTime;
    int endTime;
};
#ifdef DEBUG
void printRequest(Request request) {
    std::cout << "carId: " << request.carId << ", speed: " << request.carSpeed
              << ", routeID: " << request.routeId
              << ", start: " << request.startTime
              << ", end: " << request.endTime << std::endl;
}
#endif
bool compareRequest(Request a, Request b) {
    return a.startTime < b.startTime; // 降序排列
}
int main(int argc, char *argv[]) {
    // TODO check roadsite definition constrain
    // TODO check request constrain
    if (argc != 3) {
        std::cout
            << "Usage: [binary file] [lane definition file] [request file]\n";
        return 1;
    }
    std::ifstream laneFile;
    laneFile.open(argv[1]);
    if (!laneFile) {
        std::cout << "Unable to open the " << argv[1] << "\n";
        return 1;
    }
    Intersection intersection;
    laneFile >> intersection.id >> intersection.numberOfLane;
    for (int i = 0; i < intersection.numberOfLane; i++) {
        Lane lane;
        lane.id = i;
        lane.remove = false;
        lane.unhandledRequest = 0;
        intersection.lanes.push_back(lane);
    }
    int routeLen;
    laneFile >> routeLen;
    for (int i = 0; i < routeLen; i++) {
        Route route;
        laneFile >> route.id >> route.startLaneId >> route.endLaneId >>
            route.length;
        intersection.routes.push_back(route);
        intersection.lanes[route.startLaneId].routes.push_back(route);
    }
    std::vector<std::vector<int>> priority;
    priority.reserve(routeLen);
    for (int i = 0; i < routeLen; i++) {
        priority[i].reserve(routeLen);
    }
    for (int i = 0; i < routeLen; i++) {
        for (int j = 0; j < routeLen; j++) {
            laneFile >> priority[i][j];
        }
    }
    laneFile.close();
#ifdef DEBUG
    // ------------output read file------------
    std::cout << intersection.id << " " << intersection.numberOfLane << " "
              << routeLen << std::endl;
    for (int i = 0; i < intersection.routes.size(); i++) {
        std::cout << intersection.routes[i].id << " "
                  << intersection.routes[i].startLaneId << " "
                  << intersection.routes[i].endLaneId << " "
                  << intersection.routes[i].length << std::endl;
    }
    for (int i = 0; i < routeLen; i++) {
        for (int j = 0; j < routeLen; j++) {
            std::cout << priority[i][j] << " ";
        }
        std::cout << std::endl;
    }
    for (int i = 0; i < intersection.numberOfLane; i++) {
        std::cout << "[lane] id: " << intersection.lanes[i].id
                  << ", remove: " << intersection.lanes[i].remove << std::endl;
    }
// ----------------end output--------------
#endif
    std::ifstream requestFile;
    requestFile.open(argv[2]);
    if (!requestFile) {
        std::cout << "Unable to open the " << argv[2] << "\n";
        return 1;
    }
    std::vector<Request> requests;
    while (!requestFile.eof()) {
        Request request;
        requestFile >> request.roadsiteId >> request.carId >>
            request.carLength >> request.carSpeed >> request.frontCarId >>
            request.distanceBetweenFrontCar >> request.routeId >>
            request.startTime >> request.endTime;
        request.laneId = intersection.routes[request.routeId].startLaneId;
        intersection.lanes[request.laneId].unhandledRequest += 1;
        requests.push_back(request);
    }
    std::sort(requests.begin(), requests.end(), compareRequest);
#ifdef DEBUG
    for (int i = 0; i < requests.size(); i++) {
        printRequest(requests[i]);
    }
#endif
    std::vector<std::vector<int>> conflictEdge(
        intersection.numberOfLane,
        std::vector<int>(intersection.numberOfLane, 0));

    // conflictEdge.reserve(intersection.numberOfLane);
    // for (int i = 0; i < intersection.numberOfLane; i++) {
    //     conflictEdge[i].reserve(intersection.numberOfLane);

    // }
    std::vector<Response> responses;
    int nextIdx = 0;
    while (nextIdx < requests.size()) {
        std::cout << "nextId: " << nextIdx << std::endl;
        int currentIdx = nextIdx + 1;
        int lastTime = requests[nextIdx].endTime;
        std::vector<Request> conflicts;
        conflicts.push_back(requests[nextIdx]);
        for (int k = 0; k < intersection.numberOfLane; k++) {
            intersection.lanes[k].averageCost = 0;
            intersection.lanes[k].outDegree = 0;
            intersection.lanes[k].totalCost = 0;
            for (int j = k + 1; j < intersection.numberOfLane; j++) {
                conflictEdge[k][j] = 0;
                conflictEdge[j][k] = 0;
            }
        }
        int conflictPossibleIdx = 0;
        while (currentIdx < requests.size() &&
               requests[currentIdx].startTime <= lastTime) {
            std::cout << "currentId: " << currentIdx << std::endl;
            if (intersection
                    .lanes[intersection.routes[requests[currentIdx].routeId]
                               .startLaneId]
                    .remove) {
                Response response;
                response.routeId = requests[currentIdx].routeId;
                response.carId = requests[currentIdx].carId;
                response.status = Response::Status::rejected;
                response.startTime = -1;
                response.endTime = -1;
                responses.push_back(response);
                intersection.lanes[requests[currentIdx].laneId]
                    .unhandledRequest -= 1;
            } else {
                conflicts.push_back(requests[currentIdx]);
                lastTime = std::max(lastTime, requests[currentIdx].endTime);
                for (int k = conflictPossibleIdx; k < conflicts.size() - 1;
                     k++) {
                    if (conflicts[k].endTime <=
                        requests[currentIdx].startTime) {
                        if (priority[conflicts[k].routeId]
                                    [requests[currentIdx].routeId] != 0) {
                            int laneId1 =
                                intersection.routes[conflicts[k].routeId]
                                    .startLaneId;
                            int laneId2 =
                                intersection
                                    .routes[requests[currentIdx].routeId]
                                    .startLaneId;
                            if (conflictEdge[laneId1][laneId2] == 0) {
                                conflictEdge[laneId1][laneId2] = 1;
                                conflictEdge[laneId2][laneId1] = 1;
                                intersection.lanes[laneId1].totalCost +=
                                    intersection.lanes[laneId2]
                                        .unhandledRequest;
                                intersection.lanes[laneId1].outDegree += 1;
                                intersection.lanes[laneId2].totalCost +=
                                    intersection.lanes[laneId1]
                                        .unhandledRequest;
                                intersection.lanes[laneId2].outDegree += 1;
                            }
                        } else if (conflicts[k].routeId ==
                                       requests[currentIdx].routeId &&
                                   conflicts[k].startTime >=
                                       requests[currentIdx].startTime) {
                            conflicts.pop_back();
                            Response response;
                            response.routeId = requests[currentIdx].routeId;
                            response.carId = requests[currentIdx].carId;
                            response.status = Response::Status::rejected;
                            response.startTime = -1;
                            response.endTime = -1;
                            responses.push_back(response);
                            intersection.lanes[requests[currentIdx].laneId]
                                .unhandledRequest -= 1;
                        }
                    } else {
                        conflictPossibleIdx = k;
                    }
                }
            }
            currentIdx += 1;
        }
// TODO remove node
#ifdef DEBUG
        for (std::vector<Request>::iterator it = conflicts.begin();
             it != conflicts.end(); it++) {
            printRequest(*it);
        }
#endif
        nextIdx = currentIdx;
    }
    return 0;
}