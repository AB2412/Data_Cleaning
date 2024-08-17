# Creator: Alex Kuzmenko

def testing_cleaning
  begin
    p F::Constants::STATES_SHORT
    p F::Constants::STATES_LONG
  rescue Mysql2::Error => e
    puts "!!\nMysql2 Exception\n!!"
    p e
  rescue Exception => e
    puts "!!\nCommon Exception\n!!"
    p e
  ensure
  end
  puts 'Done 3'
end
