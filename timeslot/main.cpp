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
    // TODO check argc valid
    // TODO check file open successfully
    // TODO check roadsite definition constrain
    // TODO check request constrain
    std::ifstream laneFile;
    laneFile.open(argv[1]);
    Intersection intersection;
    laneFile >> intersection.id >> intersection.numberOfLane;
    for (int i = 0; i < intersection.numberOfLane; i++) {
        Lane lane;
        lane.id = i;
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
// ----------------end output--------------
#endif
    std::ifstream requestFile;
    requestFile.open(argv[2]);
    std::vector<Request> requests;
    while (!requestFile.eof()) {
        Request request;
        requestFile >> request.roadsiteId >> request.carId >>
            request.carLength >> request.carSpeed >> request.frontCarId >>
            request.distanceBetweenFrontCar >> request.routeId >>
            request.startTime >> request.endTime;
        requests.push_back(request);
    }
    std::sort(requests.begin(), requests.end(), compareRequest);
#ifdef DEBUG
    for (int i = 0; i < requests.size(); i++) {
        printRequest(requests[i]);
    }
#endif

    return 0;
}