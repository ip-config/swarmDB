// Copyright (C) 2018 Bluzelle
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License, version 3,
// as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

#include <node/session.hpp>
#include <sstream>


using namespace bzn;

session::session(
        std::shared_ptr<bzn::asio::io_context_base> io_context,
        bzn::session_id session_id,
        boost::asio::ip::tcp::endpoint ep,
        std::shared_ptr<bzn::chaos_base> chaos,
        bzn::protobuf_handler proto_handler,
        std::chrono::milliseconds ws_idle_timeout
)
        : session_id(session_id)
        , ep(std::move(ep))
        , io_context(std::move(io_context))
        , chaos(std::move(chaos))
        , proto_handler(std::move(proto_handler))
        , idle_timer(this->io_context->make_unique_steady_timer())
        , ws_idle_timeout(std::move(ws_idle_timeout))
        , write_buffer(nullptr, 0)
{
    LOG(debug) << "creating session " << std::to_string(session_id);
}

void
session::start_idle_timeout()
{
    this->activity = false;

    this->idle_timer->expires_from_now(this->ws_idle_timeout);
    this->idle_timer->async_wait(
            [self = shared_from_this()](auto /*ec*/)
            {
                if (!self->activity)
                {
                    LOG(info) << "Closing session " << std::to_string(self->session_id) << " due to inactivity";
                    self->close();
                    return;
                }

                self->start_idle_timeout();
            });
}

void
session::open_connection(std::shared_ptr<bzn::beast::websocket_base> ws_factory)
{
    this->start_idle_timeout();

    std::shared_ptr<bzn::asio::tcp_socket_base> socket = this->io_context->make_unique_tcp_socket();
    socket->async_connect(this->ep,
                          [self = shared_from_this(), socket, ws_factory](const boost::system::error_code& ec)
                          {
                              self->activity = true;

                              if (ec)
                              {
                                  LOG(error) << "failed to connect to: " << self->ep.address().to_string() << ":" << self->ep.port() << " - " << ec.message();

                                  return;
                              }

                              // we've completed the handshake...

                              std::lock_guard<std::mutex> lock(self->socket_lock);
                              self->websocket = ws_factory->make_unique_websocket_stream(socket->get_tcp_socket());
                              self->websocket->async_handshake(self->ep.address().to_string(), "/",
                                                  [self, ws_factory](const boost::system::error_code& ec)
                                                  {
                                                      self->activity = true;

                                                      if (ec)
                                                      {
                                                          LOG(error) << "handshake failed: " << ec.message();

                                                          return;
                                                      }

                                                      self->do_read();
                                                      self->do_write();
                                                  });
                          });
}

void
session::accept_connection(std::shared_ptr<bzn::beast::websocket_stream_base> ws)
{
    this->start_idle_timeout();

    std::lock_guard<std::mutex> lock(this->socket_lock);
    this->websocket = std::move(ws);
    this->websocket->async_accept(
            [self = shared_from_this()](boost::system::error_code ec)
            {
                self->activity = true;

                if (ec)
                {
                    LOG(error) << "websocket accept failed: " << ec.message();
                    return;
                }

                self->do_read();
                self->do_write();
            }
    );
}

void
session::do_read()
{
    auto buffer = std::make_shared<boost::beast::multi_buffer>();
    std::lock_guard<std::mutex> lock(this->socket_lock);

    if (this->reading || !this->is_open())
    {
        return;
    }

    this->reading = true;

    this->websocket->async_read(
            *buffer, [self = shared_from_this(), buffer](boost::system::error_code ec, auto /*bytes_transferred*/)
            {
                self->activity = true;

                if(ec)
                {
                    // don't log close of websocket...
                    if (ec != boost::beast::websocket::error::closed && ec != boost::asio::error::eof)
                    {
                        LOG(error) << "websocket read failed: " << ec.message();
                    }
                    self->close();
                    return;
                }

                // get the message...
                std::stringstream ss;
                ss << boost::beast::buffers(buffer->data());

                bzn_envelope proto_msg;

                if (proto_msg.ParseFromIstream(&ss))
                {
                    self->io_context->post(std::bind(self->proto_handler, proto_msg, self));
                }
                else
                {
                    LOG(error) << "Failed to parse incoming message";
                }

                self->reading = false;
                self->do_read();
            }
    );
}

void
session::do_write()
{
    // because of this mutex
    std::lock_guard<std::mutex> lock(this->socket_lock);

    // at most one concurrent invocation can pass this check
    if(this->writing || !this->is_open() || this->write_queue.empty())
    {
        return;
    }

    // and set this flag
    this->writing = true;

    auto msg = this->write_queue.front();
    this->write_queue.pop_front();

    // so there will only be one instance of this callback
    this->websocket->binary(true);
    this->write_buffer = boost::asio::buffer(*msg);
    this->websocket->async_write(this->write_buffer,
        [self = shared_from_this(), msg](boost::system::error_code ec, auto /*bytes_transferred*/)
        {
            self->activity = true;

            if(ec)
            {
                // don't log close of websocket...
                if (ec != boost::beast::websocket::error::closed && ec != boost::asio::error::eof)
                {
                    LOG(error) << "websocket read failed: " << ec.message();
                }

                {
                    std::lock_guard<std::mutex> lock(self->socket_lock);
                    self->write_queue.push_front(msg);
                }

                self->close();
                return;
            }

            // and the flag will only be reset once after each sucessful write
            self->writing = false;
            /* multiple threads may race to perform the next do_write, but we don't care which wins. If there are no
             * others then ours definitely works because we don't try until after resetting the flag. Our resetting
             * the flag can't interfere with another do_write because no such do_write can happen until we reset the
             * flag.
             */

            self->do_write();
        });
}

void
session::send_message(std::shared_ptr<bzn::encoded_message> msg)
{
    if (this->chaos->is_message_delayed())
    {
        LOG(debug) << "chaos testing delaying message";
        this->chaos->reschedule_message(std::bind(static_cast<void(session::*)(std::shared_ptr<std::string>l)>(&session::send_message), shared_from_this(), std::move(msg)));
        return;
    }

    if (this->chaos->is_message_dropped())
    {
        LOG(debug) << "chaos testing dropping message";
        return;
    }

    {
        std::lock_guard<std::mutex> lock(this->socket_lock);
        this->write_queue.push_back(msg);
    }

    this->do_write();
}

void
session::close()
{
    // TODO: re-open socket later if we still have messages to send?
    LOG(info) << "closing session";

    std::lock_guard<std::mutex> lock(this->socket_lock);
    if (this->closing)
    {
        return;
    }

    this->closing = true;
    LOG(debug) << "closing session " << std::to_string(this->session_id);

    if (this->websocket->is_open())
    {
        this->websocket->async_close(boost::beast::websocket::close_code::normal,
            [self = shared_from_this()](auto ec)
            {
                if (ec)
                {
                    LOG(error) << "failed to close websocket: " << ec.message();
                }
            });
    }
}

bool
session::is_open() const
{
    return this->websocket && this->websocket->is_open() && !this->closing;
}

session::~session()
{
    if (!this->write_queue.empty())
    {
        LOG(warning) << "dropping session with " << this->write_queue.size() << " messages left in its write queue";
    }
}