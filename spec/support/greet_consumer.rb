count_workers 3

before_fork do
  # do stuff
end

after_fork do
  # do stuff
end

worker do |payload|
  sleep 1
  "Hello, #{payload}"
end
