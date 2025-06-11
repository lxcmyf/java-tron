#!/bin/bash

API_URL="http://localhost:8090/wallet/getaccount"

count=0

while IFS= read -r address && [ $count -lt 1000 ]; do

            address=$(echo "$address" | xargs)


                if [ -n "$address" ]; then

                                response=$(curl -s "${API_URL}?address=${address}")


                                        echo "Address: $address"
                                                echo "Response: $response"
                                                        echo "------------------------"


                                                                ((count++))
                                                                    fi
                                                            done < "account_random.log"

                                                            echo "已完成 $count 次API调用"