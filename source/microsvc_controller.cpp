//
//  Created by Ivan Mejia on 12/24/16.
//
// MIT License
//
// Copyright (c) 2016 ivmeroLabs.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//

#include <std_micro_service.hpp>
#include "microsvc_controller.hpp"
#include "boost/date_time/posix_time/posix_time.hpp" //include all types plus i/o
#include <cstdlib>
#include <streambuf>

using namespace web;
using namespace http;
using namespace boost::posix_time;

void MicroserviceController::initRestOpHandlers() {
    _listener.support(methods::GET, std::bind(&MicroserviceController::handleGet, this, std::placeholders::_1));
    _listener.support(methods::PUT, std::bind(&MicroserviceController::handlePut, this, std::placeholders::_1));
    _listener.support(methods::POST, std::bind(&MicroserviceController::handlePost, this, std::placeholders::_1));
    _listener.support(methods::DEL, std::bind(&MicroserviceController::handleDelete, this, std::placeholders::_1));
    _listener.support(methods::PATCH, std::bind(&MicroserviceController::handlePatch, this, std::placeholders::_1));
}

void MicroserviceController::handleGet(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::POST));
}

void MicroserviceController::handlePatch(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::PATCH));
}

void MicroserviceController::handlePut(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::PUT));
}

void MicroserviceController::handlePost(http_request message) {
    auto path = requestPath(message);
    if (!path.empty()) {
        if (path[0] == "train") {
            train(message);
        } else if (path[0] == "predict") {
            predict(message);
        } else if (path[0] == "result") {
            result(message);
        }
    }
    else {
        message.reply(status_codes::NotFound);
    }
}

void MicroserviceController::handleDelete(http_request message) {    
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::DEL));
}

void MicroserviceController::handleHead(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::HEAD));
}

void MicroserviceController::handleOptions(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::OPTIONS));
}

void MicroserviceController::handleTrace(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::TRCE));
}

void MicroserviceController::handleConnect(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::CONNECT));
}

void MicroserviceController::handleMerge(http_request message) {
    message.reply(status_codes::NotImplemented, responseNotImpl(methods::MERGE));
}

json::value MicroserviceController::responseNotImpl(const http::method & method) {
    auto response = json::value::object();
    response["error"] = json::value::string("Method not defined");
    response["http_method"] = json::value::string(method);
    return response ;
}

void MicroserviceController::train(http_request message) {
    message.extract_json().then([=](json::value request) {
        try {
            auto filename = genFilename("train");
            auto data = request.at("data").as_array();
            std::ofstream content(filename);
            for (auto elem: data) {
                auto line = elem.as_string();
                content << line << std::endl;
            }
            content.close();

            int fail = std::system((std::string("pachctl put file training@master:") + filename + std::string(" -f ") + filename).c_str());
            if(!fail) {
                auto response = json::value::object();
                response["filename"] = json::value::string(filename);
                message.reply(status_codes::OK, response);
            } else {
                message.reply(status_codes::InternalError);
            }
        } catch(json::json_exception & e) {
            message.reply(status_codes::BadRequest);
        }
    });
}

void MicroserviceController::predict(http_request message) {
    message.extract_json().then([=](json::value request) {
        try {
            auto filename = genFilename("predict");
            auto data = request.at("data").as_array();
            std::ofstream content(filename);
            for (auto elem: data) {
                auto line = elem.as_string();
                content << line << std::endl;
            }
            content.close();

            int fail = std::system((std::string("pachctl put file streaming@master:") + filename + std::string(" -f ") + filename).c_str());
            if(!fail) {
                auto response = json::value::object();
                response["filename"] = json::value::string(filename);
                message.reply(status_codes::OK, response);
            } else {
                message.reply(status_codes::InternalError);
            }
        } catch(json::json_exception & e) {
            message.reply(status_codes::BadRequest);
        }
    });
}

void MicroserviceController::result(http_request message) {
    message.extract_json().then([=](json::value request) {
        try {
            auto filename = request.at("filename").as_string();

            int fail = std::system((std::string("pachctl get file predict@master:") + filename).c_str());
            if(!fail) {
                std::ifstream input(filename);
                std::string content((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
                input.close();

                auto response = json::value::object();
                response["data"] = json::value::string(content);
                response["ready"] = json::value::boolean(true);
                message.reply(status_codes::OK, response);
            } else {
                auto response = json::value::object();
                response["ready"] = json::value::boolean(false);
                message.reply(status_codes::OK, response);
            }
        } catch(json::json_exception & e) {
            message.reply(status_codes::BadRequest);
        }
    });
}

utility::string_t MicroserviceController::genFilename(const char *prefix) {
    utility::string_t result(prefix);
    result += '_';
    result += to_iso_string(second_clock::universal_time());
    return result;
}
