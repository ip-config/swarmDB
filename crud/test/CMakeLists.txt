set(test_srcs raft_crud_test.cpp crud_test.cpp subscription_manager_test.cpp)
set(test_libs crud node storage bootstrap raft pbft proto ${Protobuf_LIBRARIES})

add_gmock_test(crud)
