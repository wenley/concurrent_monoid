Assumes you have a running redis cluster...

```
git clone git@github.com:bbuchalter/concurrent_monoid.git
cd concurrent_monoid
bundle install
bin/sidekiq -r ./demo.rb
```
