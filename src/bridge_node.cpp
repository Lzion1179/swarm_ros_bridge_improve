/**
 * @file bridge_node.cpp
 * @author Peixuan Shu (shupeixuan@qq.com)
 * @brief Reliable TCP bridge for ros data transfer in unstable network.
 * It will send/receive the specified ROS topics in ../config/ros_topics.yaml
 * It uses zmq socket(PUB/SUB mode), which reconnects others autonomously and
 * supports 1-N pub-sub connection even with TCP protocol.
 *
 * Note: This program relies on ZMQPP (c++ wrapper around ZeroMQ).
 *  sudo apt install libzmqpp-dev
 *
 * Core Idea: It would create the receving thread for each receiving ROS topic
 *  and send ROS messages in each sub_cb() callback.
 *
 * @version 1.0
 * @date 2023-01-01
 *
 * @license BSD 3-Clause License
 * @copyright (c) 2023, Peixuan Shu
 * All rights reserved.
 *
 */

#include "bridge_node.hpp"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/select.h>
#include <unistd.h>
#include <set>
#include <algorithm>

/* Detect all local non-loopback IPv4 addresses */
std::vector<std::string> get_self_ips()
{
  std::vector<std::string> ips;
  struct ifaddrs *ifaddr, *ifa;
  if (getifaddrs(&ifaddr) == -1)
  {
    ROS_WARN("[bridge node] Failed to get network interfaces");
    return ips;
  }
  for (ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next)
  {
    if (ifa->ifa_addr == nullptr)
      continue;
    if (ifa->ifa_addr->sa_family == AF_INET)
    {
      char host[NI_MAXHOST];
      int ret = getnameinfo(ifa->ifa_addr, sizeof(struct sockaddr_in),
                            host, NI_MAXHOST, nullptr, 0, NI_NUMERICHOST);
      if (ret == 0)
      {
        std::string ip(host);
        if (ip != "127.0.0.1")
        {
          ips.push_back(ip);
        }
      }
    }
  }
  freeifaddrs(ifaddr);
  return ips;
}

/* Ping an IP address to check if it's reachable on the LAN */
bool ping_ip(const std::string &ip, int timeout_sec)
{
  std::string cmd = "ping -c 1 -W " + std::to_string(timeout_sec) + " " + ip + " > /dev/null 2>&1";
  return (system(cmd.c_str()) == 0);
}

/* Check if a TCP port is open on a remote host (non-blocking connect with timeout) */
bool check_tcp_port(const std::string &ip, int port, int timeout_sec)
{
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0)
    return false;

  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  if (inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) <= 0)
  {
    close(sock);
    return false;
  }

  // Set non-blocking for timeout control
  int flags = fcntl(sock, F_GETFL, 0);
  fcntl(sock, F_SETFL, flags | O_NONBLOCK);

  bool result = false;
  int ret = connect(sock, (struct sockaddr*)&addr, sizeof(addr));
  if (ret == 0)
  {
    result = true;  // Connected immediately (unlikely but possible on localhost)
  }
  else if (errno == EINPROGRESS)
  {
    fd_set fdset;
    FD_ZERO(&fdset);
    FD_SET(sock, &fdset);
    struct timeval tv = {timeout_sec, 0};
    if (select(sock + 1, NULL, &fdset, NULL, &tv) > 0)
    {
      int so_error = 0;
      socklen_t len = sizeof(so_error);
      getsockopt(sock, SOL_SOCKET, SO_ERROR, &so_error, &len);
      result = (so_error == 0);
    }
  }
  close(sock);
  return result;
}

/* Global state for connectivity monitoring */
bool connectivity_monitor_flag = false;
std::thread connectivity_monitor_thread;
std::map<std::string, bool> connectivity_status;
std::map<std::string, std::vector<int>> host_connect_ports;
std::vector<std::string> g_self_ips;

/* Background thread: periodically check connectivity, report state changes */
void connectivity_monitor_func()
{
  const int check_interval_sec = 5;
  // Wait for initial startup prints to finish
  std::this_thread::sleep_for(std::chrono::seconds(2));

  while (connectivity_monitor_flag)
  {
    bool changed = false;
    for (auto iter = ip_xml.begin(); iter != ip_xml.end(); ++iter)
    {
      std::string host_name = iter->first;
      std::string host_ip = iter->second;
      if (host_ip == "*")
        continue;

      // Use TCP port check: try each configured port for this host
      bool reachable = false;
      auto hp_it = host_connect_ports.find(host_name);
      if (hp_it != host_connect_ports.end() && !hp_it->second.empty())
      {
        for (int port : hp_it->second)
        {
          if (check_tcp_port(host_ip, port))
          {
            reachable = true;
            break;
          }
        }
      }
      else
      {
        // No specific recv_topics for this host, try all known ports from other hosts
        for (const auto &pair : host_connect_ports)
        {
          for (int port : pair.second)
          {
            if (check_tcp_port(host_ip, port))
            {
              reachable = true;
              break;
            }
          }
          if (reachable) break;
        }
      }
      auto it = connectivity_status.find(host_name);

      if (it == connectivity_status.end())
      {
        // First check in this thread (should not normally happen after init)
        connectivity_status[host_name] = reachable;
      }
      else if (it->second != reachable)
      {
        // State transition detected
        changed = true;
        if (reachable)
        {
          std::cout << "\r\033[32m[RECONNECTED] " << host_name
                    << " (" << host_ip << ")" << " is now connected!\033[0m " << std::endl;
        }
        else
        {
          std::cout << "\r\033[31m[DISCONNECTED] " << host_name
                    << " (" << host_ip << ")" << " connection lost!\033[0m " << std::endl;
        }
        connectivity_status[host_name] = reachable;
      }
    }

    // If any state changed, print full connectivity summary
    if (changed)
    {
      std::cout << "\r\033[34m-----connectivity check-------\033[0" << std::endl;
      for (auto iter = ip_xml.begin(); iter != ip_xml.end(); ++iter)
      {
        std::string host_name = iter->first;
        std::string host_ip = iter->second;
        if (host_ip == "*") continue;

        bool reachable = connectivity_status[host_name];
        if (reachable) {
          std::cout << "\r\033[32m[ OK ] " << host_name << " (" << host_ip << ")"
                  << " is connected\033[0m" << std::endl;
        } else {
          std::cout << "\r\033[31m[FAIL] " << host_name << " (" << host_ip << ")"
                  << " is not connected\033[0m"<< std::endl;
        }
      }
    }
      // Sleep between rounds, but break early on shutdown
      for (int i = 0; i < check_interval_sec && connectivity_monitor_flag; ++i)
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }

  // /* send messages frequency control */
  // // this is the original freq control func that has been deprecated
  // bool send_freq_control(int i)
  // {
  //   ros::Time t_now = ros::Time::now();
  //   bool discard_flag;
  //   if ((t_now - sub_t_last[i]).toSec() * sendTopics[i].max_freq < 1.0) {
  //     discard_flag = true;
  //   }
  //   else {
  //     discard_flag = false;
  //     sub_t_last[i] = t_now;
  //   }
  //   return discard_flag; // flag of discarding this message
  // }

  /* send messages frequency control */
  bool send_freq_control(int i)
  {
    bool discard_flag;
    ros::Time t_now = ros::Time::now();
    // check whether the send of this message will exceed the freq limit in the last period
    if ((send_num[i] + 1) / (t_now - sub_t_last[i]).toSec() > sendTopics[i].max_freq)
    {
      discard_flag = true;
    }
    else
    {
      discard_flag = false;
      send_num[i]++;
    }
    // freq control period (1s)
    if ((t_now - sub_t_last[i]).toSec() > 1.0)
    {
      sub_t_last[i] = t_now;
      send_num[i] = 0;
    }
    return discard_flag; // flag of discarding this message
  }

  /* uniform callback functions for ROS subscribers */
  template <typename T, int i>
  void sub_cb(const T &msg)
  {
    /* frequency control */
    auto ignore_flag = send_freq_control(i);
    if (ignore_flag)
    {
      return; // discard this message sending, abort
    }

    /* serialize the sending messages into send_buffer */
    namespace ser = ros::serialization;
    size_t data_len = ser::serializationLength(msg);             // bytes length of msg
    std::unique_ptr<uint8_t> send_buffer(new uint8_t[data_len]); // create a dynamic length array
    ser::OStream stream(send_buffer.get(), data_len);
    ser::serialize(stream, msg);

    /* zmq send message */
    zmqpp::message send_array;
    send_array << data_len;
    /* equal to:
      send_array.add_raw(reinterpret_cast<void const*>(&data_len), sizeof(size_t));
    */
    send_array.add_raw(reinterpret_cast<void const *>(send_buffer.get()), data_len);
    // std::cout << "ready send!" << std::endl;
    // send(&, true) for non-blocking, send(&, false) for blocking
    bool dont_block = false; // Actually for PUB mode zmq socket, send() will never block
    senders[i]->send(send_array, dont_block);
    // std::cout << "send!" << std::endl;

    // std::cout << msg << std::endl;
    // std::cout << i << std::endl;
  }

  /* uniform deserialize and publish the receiving messages */
  template <typename T>
  void deserialize_pub(uint8_t *buffer_ptr, size_t msg_size, int i)
  {
    T msg;
    // deserialize the receiving messages into ROS msg
    namespace ser = ros::serialization;
    ser::IStream stream(buffer_ptr, msg_size);
    ser::deserialize(stream, msg);
    // publish ROS msg
    topic_pubs[i].publish(msg);
  }

  /* receive thread function to receive messages and publish them */
  void recv_func(int i)
  {
    while (recv_thread_flags[i])
    {
      /* receive and process message */
      zmqpp::message recv_array;
      bool recv_flag; // receive success flag
      // std::cout << "ready receive!" << std::endl;
      // receive(&,true) for non-blocking, receive(&,false) for blocking
      bool dont_block = false; // 'true' leads to high cpu load
      if (recv_flag = receivers[i]->receive(recv_array, dont_block))
      {
        // std::cout << "receive!" << std::endl;
        size_t data_len;
        recv_array >> data_len; // unpack meta data
        /*  equal to:
          recv_array.get(&data_len, recv_array.read_cursor++);
          void get(T &value, size_t const cursor){
            uint8_t const* byte = static_cast<uint8_t const*>(raw_data(cursor));
            b = *byte;}
        */
        // a dynamic length array by unique_ptr
        std::unique_ptr<uint8_t> recv_buffer(new uint8_t[data_len]);
        // continue to copy the raw_data of recv_array into buffer
        memcpy(recv_buffer.get(), static_cast<const uint8_t *>(recv_array.raw_data(recv_array.read_cursor())), data_len);
        deserialize_publish(recv_buffer.get(), data_len, recvTopics[i].type, i);

        // std::cout << data_len << std::endl;
        // std::cout << recv_buffer.get() << std::endl;
      }

      /* if receive() does not block, sleep to decrease loop rate */
      if (dont_block)
        std::this_thread::sleep_for(std::chrono::microseconds(1000)); // sleep for us
      else
      {
        /* check and report receive state */
        if (recv_flag != recv_flags_last[i])
        {
          std::string topicName = recvTopics[i].name;
          if (topicName.at(0) != '/')
          {
            if (ns == "/")
            {
              topicName = "/" + topicName;
            }
            else
            {
              topicName = ns + "/" + topicName;
            }
          } // print namespace prefix if topic name is not global
          ROS_INFO("[bridge node] \"%s\" received!", topicName.c_str());
        } // false -> true(first message in)
        recv_flags_last[i] = recv_flag;
      }
    }
    return;
  }

  /* close recv socket, unsubscribe ROS topic */
  void stop_send(int i)
  {
    // senders[i]->unbind(std::string const &endpoint);
    senders[i]->close();      // close the send socket
    topic_subs[i].shutdown(); // unsubscribe
  }

  /* stop recv thread, close recv socket, unadvertise ROS topic */
  void stop_recv(int i)
  {
    recv_thread_flags[i] = false; // finish recv_func()
    // receivers[i]->disconnect(std::string &endpoint);
    receivers[i]->close();    // close the receive socket
    topic_pubs[i].shutdown(); // unadvertise
  }

  // TODO: generate or delete topic message transfers through a remote zmq service.

  int main(int argc, char **argv)
  {
    ros::init(argc, argv, "swarm_bridge");
    ros::NodeHandle nh("~");
    ros::NodeHandle nh_public;
    ns = ros::this_node::getNamespace(); // namespace of this node

    // std::cout << "--------[bridge_node]-------" << std::endl;
    // std::cout << "namespaces=" << ns << std::endl;

    //************************ Parse configuration file **************************
    // get hostnames and IPs
    if (nh.getParam("IP", ip_xml) == false)
    {
      ROS_ERROR("[bridge node] No IP found in the configuration!");
      return 1;
    }
    // get "send topics" params (topic_name, topic_type, IP, port)
    if (nh.getParam("send_topics", send_topics_xml))
    {
      ROS_ASSERT(send_topics_xml.getType() == XmlRpc::XmlRpcValue::TypeArray);
      len_send = send_topics_xml.size();
    }
    else
    {
      ROS_WARN("[bridge node] No send_topics found in the configuration!");
      len_send = 0;
    }
    // get "receive topics" params (topic_name, topic_type, IP, port)
    if (nh.getParam("recv_topics", recv_topics_xml))
    {
      ROS_ASSERT(recv_topics_xml.getType() == XmlRpc::XmlRpcValue::TypeArray);
      len_recv = recv_topics_xml.size();
    }
    else
    {
      ROS_WARN("[bridge node] No recv_topics found in the configuration!");
      len_recv = 0;
    }

    if (len_send > SUB_MAX)
    {
      ROS_FATAL("[bridge_node] The number of send topics in configuration exceeds the limit %d!", SUB_MAX);
      return 2;
    }

    // 取消IP打印
    // std::cout << "-------------IP------------" << std::endl;
    for (auto iter = ip_xml.begin(); iter != ip_xml.end(); ++iter)
    {
      std::string host_name = iter->first;
      std::string host_ip = iter->second;
      // std::cout << host_name << " : " << host_ip << std::endl;
      if (ip_map.find(host_name) != ip_map.end())
      { // ip_xml will never contain same names actually.
        ROS_WARN("[bridge node] IPs with the same name in configuration %s!", host_name.c_str());
      }
      ip_map[host_name] = host_ip;
    }

    // ******************** Self IP Detection **************************
    std::cout << "\n\r\033[34m--------self IP detected------\033[0m" << std::endl;
    g_self_ips = get_self_ips();
    if (g_self_ips.empty())
    {
      ROS_WARN("[bridge node] No network interface detected! Using loopback.");
      std::cout << "127.0.0.1 (fallback)" << std::endl;
    }
    else
    {
      for (const auto &ip : g_self_ips)
      {
        std::cout << ip << std::endl;
      }
    }

    // ******************** Connectivity Check **************************
    // Build host->ports mapping from recv_topics for TCP port detection
    host_connect_ports.clear();
    for (int32_t i = 0; i < len_recv; ++i)
    {
      ROS_ASSERT(recv_topics_xml[i].getType() == XmlRpc::XmlRpcValue::TypeStruct);
      std::string srcIP_name = recv_topics_xml[i]["srcIP"];
      int srcPort = recv_topics_xml[i]["srcPort"];
      // Only add if not already present for this host
      if (std::find(host_connect_ports[srcIP_name].begin(),
                    host_connect_ports[srcIP_name].end(), srcPort) == host_connect_ports[srcIP_name].end())
      {
        host_connect_ports[srcIP_name].push_back(srcPort);
      }
    }

    std::cout << "\r\033[34m-----connectivity check-------\033[0m" << std::endl;
    for (auto iter = ip_xml.begin(); iter != ip_xml.end(); ++iter)
    {
      std::string host_name = iter->first;
      std::string host_ip = iter->second;
      // Skip self entry (value is '*')
      if (host_ip == "*")
        continue;

      // Use TCP port check: try each configured port for this host
      bool reachable = false;
      auto hp_it = host_connect_ports.find(host_name);
      if (hp_it != host_connect_ports.end() && !hp_it->second.empty())
      {
        for (int port : hp_it->second)
        {
          if (check_tcp_port(host_ip, port))
          {
            reachable = true;
            break;
          }
        }
      }
      else
      {
        // No specific recv_topics for this host, try all known ports from other hosts
        for (const auto &pair : host_connect_ports)
        {
          for (int port : pair.second)
          {
            if (check_tcp_port(host_ip, port))
            {
              reachable = true;
              break;
            }
          }
          if (reachable) break;
        }
      }

      connectivity_status[host_name] = reachable;
      if (reachable)
      {
        std::cout << "\r\033[32m[ OK ] " << host_name << " (" << host_ip << ")"
                  << " is connected\033[0m" << std::endl;
      }
      else
      {
        std::cout << "\r\033[31m[FAIL] " << host_name << " (" << host_ip << ")"
                  << " is not connected!\033[0m" << std::endl;
      }
    }

    // Start background connectivity monitor
    connectivity_monitor_flag = true;
    connectivity_monitor_thread = std::thread(&connectivity_monitor_func);

    std::cout << "\r\033[34m--------send topics--------\033[0m" << std::endl;
    std::set<int> srcPorts; // for duplicate check
    for (int32_t i = 0; i < len_send; ++i)
    {
      ROS_ASSERT(send_topics_xml[i].getType() == XmlRpc::XmlRpcValue::TypeStruct);
      XmlRpc::XmlRpcValue send_topic_xml = send_topics_xml[i];
      std::string topic_name = send_topic_xml["topic_name"];
      std::string msg_type = send_topic_xml["msg_type"];
      int max_freq = send_topic_xml["max_freq"];
      std::string srcIP = ip_map[send_topic_xml["srcIP"]];
      int srcPort = send_topic_xml["srcPort"];
      TopicInfo topic = {.name = topic_name, .type = msg_type, .max_freq = max_freq, .ip = srcIP, .port = srcPort};
      sendTopics.emplace_back(topic);
      // check for duplicate ports:
      if (srcPorts.find(srcPort) != srcPorts.end())
      {
        ROS_FATAL("[bridge_node] Send topics with the same srcPort %d in configuration!", srcPort);
        return 3;
      }
      srcPorts.insert(srcPort); // for duplicate check
      if (topic.name.at(0) != '/')
      {
        std::cout << ns;
        if (ns != "/")
        {
          std::cout << "/";
        }
      } // print namespace prefix if topic.name is not global
      std::cout << topic.name << "  " << topic.max_freq << "Hz(max)" << std::endl;
    }

    std::cout << "\r\033[34m-------receive topics------\033[0m" << std::endl;
    for (int32_t i = 0; i < len_recv; ++i)
    {
      ROS_ASSERT(recv_topics_xml[i].getType() == XmlRpc::XmlRpcValue::TypeStruct);
      XmlRpc::XmlRpcValue recv_topic_xml = recv_topics_xml[i];
      std::string topic_name = recv_topic_xml["topic_name"];
      std::string msg_type = recv_topic_xml["msg_type"];
      int max_freq = recv_topic_xml["max_freq"];
      std::string srcIP = ip_map[recv_topic_xml["srcIP"]];
      int srcPort = recv_topic_xml["srcPort"];
      TopicInfo topic = {.name = topic_name, .type = msg_type, .max_freq = max_freq, .ip = srcIP, .port = srcPort};
      recvTopics.emplace_back(topic);
      if (topic.name.at(0) != '/')
      {
        std::cout << ns;
        if (ns != "/")
        {
          std::cout << "/";
        }
      } // print namespace prefix if topic.name is not global
      std::cout << topic.name << "  (from " << recv_topic_xml["srcIP"] << ")" << std::endl;
    }

    // ********************* zmq socket initialize ***************************
    // send sockets (zmq socket PUB mode)
    for (int32_t i = 0; i < len_send; ++i)
    {
      const std::string url = "tcp://" + sendTopics[i].ip + ":" + std::to_string(sendTopics[i].port);
      std::unique_ptr<zmqpp::socket> sender(new zmqpp::socket(context, zmqpp::socket_type::pub));
      sender->bind(url);
      senders.emplace_back(std::move(sender)); // sender is now released by std::move
    }

    // receive sockets (zmq socket SUB mode)
    for (int32_t i = 0; i < len_recv; ++i)
    {
      // Check if target IP is local (avoid self-reception feedback loop)
      bool is_self = (std::find(g_self_ips.begin(), g_self_ips.end(), recvTopics[i].ip) != g_self_ips.end());
      std::unique_ptr<zmqpp::socket> receiver(new zmqpp::socket(context, zmqpp::socket_type::sub));
      std::string const zmq_topic = ""; // "" means all zmq topic
      receiver->subscribe(zmq_topic);
      if (!is_self)
      {
        const std::string url = "tcp://" + recvTopics[i].ip + ":" + std::to_string(recvTopics[i].port);
        receiver->connect(url);
      }
      receivers.emplace_back(std::move(receiver));
    }

    // ******************* ROS subscribe and publish *************************
    // ROS topic subsrcibe and send
    for (int32_t i = 0; i < len_send; ++i)
    {
      sub_t_last.emplace_back(ros::Time::now()); // freq control period start time
      send_num.emplace_back(0);                  // the send messages number in a period
      ros::Subscriber subscriber;
      // The uniform callback function is sub_cb()
      subscriber = topic_subscriber(sendTopics[i].name, sendTopics[i].type, nh_public, i);
      topic_subs.emplace_back(subscriber);
      // use topic_subs[i].shutdown() to unsubscribe
    }

    // ROS topic receive and publish
    for (int32_t i = 0; i < len_recv; ++i)
    {
      ros::Publisher publisher;
      publisher = topic_publisher(recvTopics[i].name, recvTopics[i].type, nh_public);
      topic_pubs.emplace_back(publisher);
    }

    // ****************** launch receive threads *****************************
    for (int32_t i = 0; i < len_recv; ++i)
    {
      recv_thread_flags.emplace_back(true); // enable receive thread flags
      recv_flags_last.emplace_back(false);  // receive success flag
      recv_threads.emplace_back(std::thread(&recv_func, i));
    }

    ros::spin();

    // ***************** stop send/receive ******************************
    for (int32_t i = 0; i < len_send; ++i)
    {
      stop_send(i);
    }

    for (int32_t i = 0; i < len_recv; ++i)
    {
      stop_recv(i);
    }

    // Stop connectivity monitor thread
    connectivity_monitor_flag = false;
    if (connectivity_monitor_thread.joinable())
      connectivity_monitor_thread.join();

    return 0;
  }
