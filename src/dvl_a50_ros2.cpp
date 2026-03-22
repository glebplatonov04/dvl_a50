#include <algorithm>
#include <exception>
#include <string>
#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <cmath>
#include <map>
#include <future>

#include <rclcpp/rclcpp.hpp>
#include <rclcpp/publisher.hpp>
#include <rclcpp_lifecycle/lifecycle_node.hpp>
#include <rclcpp_lifecycle/lifecycle_publisher.hpp>

#include <std_srvs/srv/trigger.hpp>
#include <geometry_msgs/msg/pose_with_covariance_stamped.hpp>
#include <nav_msgs/msg/odometry.hpp>
#include <tf2_geometry_msgs/tf2_geometry_msgs.hpp>
#include <marine_acoustic_msgs/msg/dvl.hpp>

#include "dvl_a50/dvl_a50.hpp"


using namespace dvl_a50;

namespace {

bool wl_json_bool(const nlohmann::json & j, bool default_v = false)
{
    try {
        if (j.is_boolean()) {
            return j.get<bool>();
        }
        if (j.is_number()) {
            return j.get<double>() != 0.0;
        }
    } catch (...) {
    }
    return default_v;
}

double wl_json_double(const nlohmann::json & j, double fallback = 0.0)
{
    try {
        if (j.is_number()) {
            return j.get<double>();
        }
    } catch (...) {
    }
    return fallback;
}

}  // namespace


class DvlA50Node : public rclcpp_lifecycle::LifecycleNode
{
public:
    DvlA50Node(std::string name)
    : rclcpp_lifecycle::LifecycleNode(name)
    {
        this->declare_parameter<std::string>("ip_address", "192.168.194.95");
        this->declare_parameter<std::string>("frame", "dvl_a50_link");
        this->declare_parameter<double>("rate", 30.0);
        this->declare_parameter<bool>("enable_on_activate", false);
        this->declare_parameter<int>("speed_of_sound", 1500);
        this->declare_parameter<bool>("led_enabled", true);
        this->declare_parameter<int>("mountig_rotation_offset", 0);
        this->declare_parameter<std::string>("range_mode", "auto");
        // Policy (Water Linked JSON): prefer nested 3×3 covariance; else flat-9; else nonnegative
        // scalar diagonal; else diag(fom²) per protocol (fom = figure of merit, m/s); else defaults.
        // See https://docs.waterlinked.com/dvl/dvl-protocol/
        this->declare_parameter<double>("velocity_covariance_fallback_var_xy", 1.0);
        this->declare_parameter<double>("velocity_covariance_fallback_var_z", 4.0);
        // Dead-reckoning pose diagonal when std missing/unparsable (interpreted like driver: per-axis entry)
        this->declare_parameter<double>("dead_reckoning_pose_covariance_fallback", 100.0);

        // Some information won't change, so we can fill it in here. 
        velocity_report.velocity_mode = marine_acoustic_msgs::msg::Dvl::DVL_MODE_BOTTOM;
        velocity_report.dvl_type = marine_acoustic_msgs::msg::Dvl::DVL_TYPE_PISTON;

        // Each beam points 22.5° away from the center, LED pointing forward. 
        // Transducers are rotated 45° around Z.
        // Beam 1 (+135° from X)
        velocity_report.beam_unit_vec[0].x = -0.6532814824381883;
        velocity_report.beam_unit_vec[0].y =  0.6532814824381883;
        velocity_report.beam_unit_vec[0].z =  0.38268343236508984;

        // Beam 2 (-135° from X)
        velocity_report.beam_unit_vec[1].x = -0.6532814824381883;
        velocity_report.beam_unit_vec[1].y = -0.6532814824381883;
        velocity_report.beam_unit_vec[1].z =  0.38268343236508984;

        // Beam 3 (-45° from X)
        velocity_report.beam_unit_vec[2].x =  0.6532814824381883;
        velocity_report.beam_unit_vec[2].y = -0.6532814824381883;
        velocity_report.beam_unit_vec[2].z =  0.38268343236508984;

        // Beam 4 (+45° from X)
        velocity_report.beam_unit_vec[3].x =  0.6532814824381883;
        velocity_report.beam_unit_vec[3].y =  0.6532814824381883;
        velocity_report.beam_unit_vec[3].z =  0.38268343236508984;
    }

    ~DvlA50Node()
    {}

    CallbackReturn on_configure(const rclcpp_lifecycle::State& state)
    {
        // Parameters
        std::string ip_address = this->get_parameter("ip_address").as_string();
        std::string frame = this->get_parameter("frame").as_string();
        rate = this->get_parameter("rate").as_double();
        RCLCPP_INFO(get_logger(), "Connecting to DVL A50 at %s", ip_address.c_str());

        int success = dvl.connect(ip_address, false);
        if (success != 0)
        {
            RCLCPP_ERROR(get_logger(), "Connection failed with error code %i", success);
            return CallbackReturn::FAILURE;
        }

        // Configure
        enable_on_activate = this->get_parameter("enable_on_activate").as_bool();
        int speed_of_sound = this->get_parameter("speed_of_sound").as_int();
        bool led_enabled = this->get_parameter("led_enabled").as_bool();
        int mountig_rotation_offset = this->get_parameter("mountig_rotation_offset").as_int();
        std::string range_mode = this->get_parameter("range_mode").as_string();
        
        dvl.configure(speed_of_sound, false, led_enabled, mountig_rotation_offset, range_mode);

        vel_cov_fb_xy_ = this->get_parameter("velocity_covariance_fallback_var_xy").as_double();
        vel_cov_fb_z_ = this->get_parameter("velocity_covariance_fallback_var_z").as_double();
        dr_pose_cov_fb_ = this->get_parameter("dead_reckoning_pose_covariance_fallback").as_double();
        
        // Set some values from parameters that won't change
        velocity_report.sound_speed = speed_of_sound;
        velocity_report.header.frame_id = frame;
        dead_reckoning_report.header.frame_id = frame;
        odometry.header.frame_id = frame;
        
        // Publishers
        velocity_pub = this->create_publisher<marine_acoustic_msgs::msg::Dvl>("dvl/velocity", 10);
        dead_reckoning_pub = this->create_publisher<geometry_msgs::msg::PoseWithCovarianceStamped>("dvl/dead_reckoning", 10);
        odometry_pub = this->create_publisher<nav_msgs::msg::Odometry>("dvl/odometry", 10);

        return CallbackReturn::SUCCESS;
    }

    CallbackReturn on_activate(const rclcpp_lifecycle::State& state)
    {
        LifecycleNode::on_activate(state);

        dvl.set_acoustic_enabled(enable_on_activate);
        
        velocity_pub->on_activate();
        dead_reckoning_pub->on_activate();
        odometry_pub->on_activate();
        
        // Services
        enable_srv = this->create_service<std_srvs::srv::Trigger>(
            "enable", 
            bind(&DvlA50Node::srv_send_param<bool>, this, "acoustic_enabled", true, std::placeholders::_1, std::placeholders::_2));

        disable_srv = this->create_service<std_srvs::srv::Trigger>(
            "disable", 
            bind(&DvlA50Node::srv_send_param<bool>, this, "acoustic_enabled", false, std::placeholders::_1, std::placeholders::_2));

        get_config_srv = this->create_service<std_srvs::srv::Trigger>(
            "get_config", 
            bind(&DvlA50Node::srv_send_command, this, "get_config", std::placeholders::_1, std::placeholders::_2));

        calibrate_gyro_srv = this->create_service<std_srvs::srv::Trigger>(
            "calibrate_gyro", 
            bind(&DvlA50Node::srv_send_command, this, "calibrate_gyro", std::placeholders::_1, std::placeholders::_2));

        reset_dead_reckoning_srv = this->create_service<std_srvs::srv::Trigger>(
            "reset_dead_reckoning", 
            bind(&DvlA50Node::srv_send_command, this, "reset_dead_reckoning", std::placeholders::_1, std::placeholders::_2));

        trigger_ping_srv = this->create_service<std_srvs::srv::Trigger>(
            "trigger_ping", 
            bind(&DvlA50Node::srv_send_command, this, "trigger_ping", std::placeholders::_1, std::placeholders::_2));

        // Start reading data
        RCLCPP_INFO(get_logger(), "Starting to receive reports at <= %f Hz", rate);
        timer = this->create_wall_timer(
            std::chrono::duration<double>(1. / rate), 
            std::bind(&DvlA50Node::publish, this)
        );

        return CallbackReturn::SUCCESS;
    }

    CallbackReturn on_deactivate(const rclcpp_lifecycle::State& state)
    {
        LifecycleNode::on_deactivate(state);
        RCLCPP_INFO(get_logger(), "Stopping report reception");

        // Stop reading data
        dvl.set_acoustic_enabled(false);
        timer->cancel();

        velocity_pub->on_deactivate();
        dead_reckoning_pub->on_deactivate();
        odometry_pub->on_deactivate();

        return CallbackReturn::SUCCESS;
    }

    CallbackReturn on_cleanup(const rclcpp_lifecycle::State& state)
    {
        dvl.disconnect();
        timer.reset();

        velocity_pub.reset();
        dead_reckoning_pub.reset();
        odometry_pub.reset();

        get_config_srv.reset();
        calibrate_gyro_srv.reset();
        reset_dead_reckoning_srv.reset();

        return CallbackReturn::SUCCESS;
    }

    CallbackReturn on_shutdown(const rclcpp_lifecycle::State& state)
    {
        return CallbackReturn::SUCCESS;
    }


    void publish()
    {
        DvlA50::Message res = dvl.receive();

        if (res.contains("response_to"))
        {
            // Command response
            std::string trigger = res["response_to"];

            // Always print the result
            if (res["success"])
            {
                RCLCPP_INFO(get_logger(), "%s success: %s", trigger.c_str(), res["result"].dump().c_str());
            }
            else
            {
                RCLCPP_ERROR(get_logger(), "%s failed: %s", trigger.c_str(), res["error_message"].dump().c_str());
            }

            // Check if we have a pending service call for this command and release it
            auto pending_it = pending_service_calls.find(trigger);
            if (pending_it != pending_service_calls.end())
            {
                pending_it->second.set_value(res);
                pending_service_calls.erase(pending_it);
            }
        }
        else if(res.contains("altitude"))
        {
            // Entire branch: any remaining nlohmann misuse (e.g. transducers shape, ROS msg assign)
            // must not abort the process.
            try {
            // Velocity report
            const uint64_t tov = static_cast<uint64_t>(wl_json_double(res["time_of_validity"]));
            velocity_report.header.stamp = rclcpp::Time(tov * 1000);

            velocity_report.velocity.x = wl_json_double(res["vx"]);
            velocity_report.velocity.y = wl_json_double(res["vy"]);
            velocity_report.velocity.z = wl_json_double(res["vz"]);

            fill_velocity_covariance_from_json(res);

            const double current_altitude = wl_json_double(res["altitude"]);
            if (current_altitude >= 0.0 && wl_json_bool(res["velocity_valid"])) {
                velocity_report.altitude = current_altitude;
            }

            velocity_report.course_gnd = std::atan2(velocity_report.velocity.y, velocity_report.velocity.x);
            velocity_report.speed_gnd = std::sqrt(
                velocity_report.velocity.x * velocity_report.velocity.x +
                velocity_report.velocity.y * velocity_report.velocity.y);

            velocity_report.beam_ranges_valid = true;
            velocity_report.beam_velocities_valid = wl_json_bool(res["velocity_valid"]);

            velocity_report.num_good_beams = 0;
            if (res.contains("transducers") && res["transducers"].is_array() &&
                res["transducers"].size() >= 4) {
                const auto & transducers = res["transducers"];
                for (size_t beam = 0; beam < 4; beam++) {
                    const auto & td = transducers[beam];
                    if (!td.is_object()) {
                        continue;
                    }
                    velocity_report.num_good_beams += wl_json_bool(td["beam_valid"]);
                    velocity_report.range = wl_json_double(td["distance"], velocity_report.range);
                    velocity_report.beam_quality = wl_json_double(td["rssi"], velocity_report.beam_quality);
                    velocity_report.beam_velocity = wl_json_double(td["velocity"], velocity_report.beam_velocity);
                }
            } else {
                RCLCPP_WARN_THROTTLE(
                    get_logger(), *get_clock(), 10000,
                    "DVL velocity report: transducers missing or not a 4-element array");
            }

            velocity_pub->publish(velocity_report);

            odometry.header.stamp = velocity_report.header.stamp;
            odometry.twist.twist.linear.x = velocity_report.velocity.x;
            odometry.twist.twist.linear.y = velocity_report.velocity.y;
            odometry.twist.twist.linear.z = velocity_report.velocity.z;

            for (size_t i = 0; i < 3; i++) {
                for (size_t j = 0; j < 3; j++) {
                    odometry.twist.covariance[i * 6 + j] =
                        velocity_report.velocity_covar[i * 3 + j];
                }
            }

            odometry_pub->publish(odometry);
            } catch (const std::exception & e) {
                RCLCPP_ERROR_THROTTLE(
                    get_logger(), *get_clock(), 2000,
                    "DVL velocity report dropped (JSON/field error): %s", e.what());
            }
        }
        else if (res.contains("pitch"))
        {
            // Dead reckoning report
            dead_reckoning_report.header.stamp = rclcpp::Time(static_cast<uint64_t>(double(res["ts"])) * 1e9);

            dead_reckoning_report.pose.pose.position.x = double(res["x"]);
            dead_reckoning_report.pose.pose.position.y = double(res["y"]);
            dead_reckoning_report.pose.pose.position.z = double(res["z"]);

            const double dr_cov_diag = parse_dr_pose_covariance_diagonal(res);
            dead_reckoning_report.pose.covariance[0] = dr_cov_diag;
            dead_reckoning_report.pose.covariance[7] = dr_cov_diag;
            dead_reckoning_report.pose.covariance[14] = dr_cov_diag;

            tf2::Quaternion quat;
            quat.setRPY(double(res["roll"]), double(res["pitch"]), double(res["yaw"]));
            dead_reckoning_report.pose.pose.orientation = tf2::toMsg(quat);

            dead_reckoning_pub->publish(dead_reckoning_report);

            // Update the pose of the odometry
            odometry.header.stamp = dead_reckoning_report.header.stamp;
            odometry.pose = dead_reckoning_report.pose;            
            odometry_pub->publish(odometry);
        }
        else
        {
            RCLCPP_WARN(get_logger(), "Received unexpected DVL response: %s", res.dump().c_str());
        }
    }


    void srv_send_command(
        std::string command,
        std_srvs::srv::Trigger::Request::SharedPtr req,
        std_srvs::srv::Trigger::Response::SharedPtr res)
    {
        std::promise<DvlA50::Message> promise;
        std::future<DvlA50::Message> future = promise.get_future();
        pending_service_calls.insert(std::make_pair(command, std::move(promise)));
        dvl.send_command(command);

        DvlA50::Message json_data = future.get();
        res->success = json_data["success"];
        if (res->success)
        {
            res->message = json_data["result"];
        }
        else
        {
            res->message = json_data["error_message"];
        }
    }

    template<typename T>
    void srv_send_param(
        std::string param,
        const T& value,
        std_srvs::srv::Trigger::Request::SharedPtr req,
        std_srvs::srv::Trigger::Response::SharedPtr res)
    {
        std::promise<DvlA50::Message> promise;
        std::future<DvlA50::Message> future = promise.get_future();
        pending_service_calls.insert(std::make_pair("set_config", std::move(promise)));
        dvl.set(param, value);

        DvlA50::Message json_data = future.get();
        res->success = json_data["success"];
        res->message = json_data["error_message"];
    }


private:
    /** Last resort: parameterized diagonal variances (m/s)². */
    void apply_velocity_covariance_fallback(const char * reason)
    {
        std::fill(
            std::begin(velocity_report.velocity_covar),
            std::end(velocity_report.velocity_covar), 0.0);
        velocity_report.velocity_covar[0] = vel_cov_fb_xy_;
        velocity_report.velocity_covar[4] = vel_cov_fb_xy_;
        velocity_report.velocity_covar[8] = vel_cov_fb_z_;
        RCLCPP_DEBUG_THROTTLE(
            get_logger(), *get_clock(), 10000, "DVL velocity covariance default (%s): diag xy=%.4g z=%.4g",
            reason, vel_cov_fb_xy_, vel_cov_fb_z_);
    }

    /** Protocol: FOM is max velocity uncertainty (m/s); use fom² as variance on each axis. */
    bool try_velocity_covariance_from_fom(const DvlA50::Message& res)
    {
        if (!res.contains("fom") || res["fom"].is_null() || !res["fom"].is_number()) {
            return false;
        }
        const double fom = res["fom"].get<double>();
        if (!std::isfinite(fom) || fom < 0.0) {
            return false;
        }
        const double v = fom * fom;
        std::fill(
            std::begin(velocity_report.velocity_covar),
            std::end(velocity_report.velocity_covar), 0.0);
        velocity_report.velocity_covar[0] = v;
        velocity_report.velocity_covar[4] = v;
        velocity_report.velocity_covar[8] = v;
        RCLCPP_DEBUG_THROTTLE(
            get_logger(), *get_clock(), 10000,
            "DVL velocity covariance from fom² (fom=%.6g m/s)", fom);
        return true;
    }

    /**
     * Policy: 3×3 nested (primary) → flat 9 → nonnegative scalar diagonal → fom² → defaults.
     * Never throws out of this function; throttled warnings on malformed input.
     */
    void fill_velocity_covariance_from_json(const DvlA50::Message& res)
    {
        std::fill(
            std::begin(velocity_report.velocity_covar),
            std::end(velocity_report.velocity_covar), 0.0);

        try {
            if (res.contains("covariance") && !res["covariance"].is_null()) {
                const auto& cov_json = res["covariance"];

                // 1) Primary: nested 3×3 (Water Linked json_v3.1 example format)
                if (cov_json.is_array() && cov_json.size() == 3 && cov_json.at(0).is_array()) {
                    bool ok3x3 = true;
                    for (size_t i = 0; i < 3 && ok3x3; ++i) {
                        if (!cov_json.at(i).is_array() || cov_json.at(i).size() < 3) {
                            ok3x3 = false;
                        }
                    }
                    if (ok3x3) {
                        for (size_t i = 0; i < 3; ++i) {
                            for (size_t j = 0; j < 3; ++j) {
                                velocity_report.velocity_covar[i * 3 + j] =
                                    cov_json.at(i).at(j).get<double>();
                            }
                        }
                        return;
                    }
                }

                // 2) Compatibility: flat length-9 row-major
                if (cov_json.is_array() && cov_json.size() == 9) {
                    for (size_t k = 0; k < 9; ++k) {
                        velocity_report.velocity_covar[k] = cov_json.at(k).get<double>();
                    }
                    return;
                }
                if (cov_json.is_array() && cov_json.size() > 9) {
                    RCLCPP_WARN_THROTTLE(
                        get_logger(), *get_clock(), 10000,
                        "DVL covariance: flat array length %zu > 9, using first 9 entries",
                        cov_json.size());
                    for (size_t k = 0; k < 9; ++k) {
                        velocity_report.velocity_covar[k] = cov_json.at(k).get<double>();
                    }
                    return;
                }

                // 3) Scalar nonnegative → diagonal (variances)
                if (cov_json.is_number()) {
                    const double v = cov_json.get<double>();
                    if (std::isfinite(v) && v >= 0.0) {
                        velocity_report.velocity_covar[0] = v;
                        velocity_report.velocity_covar[4] = v;
                        velocity_report.velocity_covar[8] = v;
                        RCLCPP_DEBUG_THROTTLE(
                            get_logger(), *get_clock(), 10000,
                            "DVL covariance: nonnegative scalar diagonal variances");
                        return;
                    }
                    RCLCPP_WARN_THROTTLE(
                        get_logger(), *get_clock(), 10000,
                        "DVL covariance: scalar invalid (negative or non-finite), trying fom / default");
                }

                if (cov_json.is_array()) {
                    RCLCPP_WARN_THROTTLE(
                        get_logger(), *get_clock(), 10000,
                        "DVL covariance: array shape not 3×3 or length 9, trying fom / default");
                } else if (!cov_json.is_number()) {
                    RCLCPP_WARN_THROTTLE(
                        get_logger(), *get_clock(), 10000,
                        "DVL covariance: unsupported JSON type, trying fom / default");
                }
            }

            // 4) Fallback: fom² on diagonal (documented link between fom and covariance)
            if (try_velocity_covariance_from_fom(res)) {
                return;
            }

            // 5) Parameterized conservative default
            apply_velocity_covariance_fallback(
                res.contains("covariance") ? "no usable covariance" : "covariance missing");
        } catch (const std::exception& e) {
            RCLCPP_WARN_THROTTLE(
                get_logger(), *get_clock(), 10000,
                "DVL covariance parse error: %s — trying fom / default", e.what());
            std::fill(
                std::begin(velocity_report.velocity_covar),
                std::end(velocity_report.velocity_covar), 0.0);
            if (try_velocity_covariance_from_fom(res)) {
                return;
            }
            apply_velocity_covariance_fallback("exception after fom");
        }
    }

    /** Same convention as original driver: value from JSON `std` on pose diagonal, or conservative fallback. */
    double parse_dr_pose_covariance_diagonal(const DvlA50::Message& res)
    {
        if (!res.contains("std") || res["std"].is_null()) {
            return dr_pose_cov_fb_;
        }
        try {
            const auto& s = res["std"];
            if (s.is_number()) {
                return s.get<double>();
            }
            if (s.is_array() && !s.empty()) {
                return s.at(0).get<double>();
            }
        } catch (const std::exception&) {
        }
        return dr_pose_cov_fb_;
    }

    DvlA50 dvl;

    double rate;
    bool enable_on_activate;
    double vel_cov_fb_xy_{1.0};
    double vel_cov_fb_z_{4.0};
    double dr_pose_cov_fb_{100.0};

    marine_acoustic_msgs::msg::Dvl velocity_report;
    geometry_msgs::msg::PoseWithCovarianceStamped dead_reckoning_report;
    nav_msgs::msg::Odometry odometry;

    // Promises of unfulfilled service calls; assumes that no service is called twice in parallel
    std::map<std::string, std::promise<DvlA50::Message>> pending_service_calls;
    
    rclcpp::TimerBase::SharedPtr timer;
    rclcpp_lifecycle::LifecyclePublisher<marine_acoustic_msgs::msg::Dvl>::SharedPtr velocity_pub;
    rclcpp_lifecycle::LifecyclePublisher<geometry_msgs::msg::PoseWithCovarianceStamped>::SharedPtr dead_reckoning_pub;
    rclcpp_lifecycle::LifecyclePublisher<nav_msgs::msg::Odometry>::SharedPtr odometry_pub;

    rclcpp::Service<std_srvs::srv::Trigger>::SharedPtr enable_srv;
    rclcpp::Service<std_srvs::srv::Trigger>::SharedPtr disable_srv;
    rclcpp::Service<std_srvs::srv::Trigger>::SharedPtr get_config_srv;
    rclcpp::Service<std_srvs::srv::Trigger>::SharedPtr calibrate_gyro_srv;
    rclcpp::Service<std_srvs::srv::Trigger>::SharedPtr reset_dead_reckoning_srv;
    rclcpp::Service<std_srvs::srv::Trigger>::SharedPtr trigger_ping_srv;
};


int main(int argc, char * argv[])
{
    // force flush of the stdout buffer.
    // this ensures a correct sync of all prints
    // even when executed simultaneously within the launch file.
    setvbuf(stdout, NULL, _IONBF, BUFSIZ);

    rclcpp::init(argc, argv);
    rclcpp::executors::SingleThreadedExecutor exe;
    std::shared_ptr<DvlA50Node> node = std::make_shared<DvlA50Node>("dvl_a50");
    exe.add_node(node->get_node_base_interface());
    exe.spin();
    rclcpp::shutdown();
    return 0;
}
