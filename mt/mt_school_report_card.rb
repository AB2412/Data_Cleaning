# Creator:      Sergii Butrymenko
# Dataset Name: Montana School Report Card
# Task #:       107
# Scrape Task:  https://lokic.locallabs.com/scrape_tasks/233
# Dataset Link: https://lokic.locallabs.com/data_sets/59
# Created:      April 2023

# ruby mlc.rb --tool="clean::mt::mt_school_report_card" --mode='schools'
# ruby mlc.rb --tool="clean::mt::mt_school_report_card" --mode='districts'
# ruby mlc.rb --tool="clean::mt::mt_school_report_card" --mode='check_unmatched'

def execute(options = {})
  table_description = {
    schools: {
      raw_table: 'mt_info_school',
      raw_column: 'school_name',
      clean_column: 'school_name_clean',
    }
  }
  route = C::Mysql.on(DB01, 'us_schools_raw')
  mode = options['mode']&.to_sym
  table_info = table_description[mode]
  case mode
  when :schools
    clean_school_names(table_info, route)
  # when :districts
  #   clean_district_names(route)
  # when :check_unmatched
  #   check_unmatched(route)
  else
    puts 'EMPTY'.black.on_yellow
  end
  route.close
end

def message_to_slack(message, type = '')
  type = case type
         when :alert
           ':error:'
         when :warning
           ':warning:'
         when :info
           ':information_source:'
         else
           ''
         end
  Slack::Web::Client.new.chat_postMessage(
    channel: 'UKLB1JGDN',
    text: "*[CLEANING #107] Montana School Report Card* \n>#{type} #{message}",
    as_user: true
  )
end

def escape(str)
  # str = str.to_s.strip.squeeze(' ')
  return str if str.nil?

  str = str.to_s
  str.gsub(/\\/, '\&\&').gsub(/'/, "''")
end

def escape_or_null(str)
  return 'NULL' if str.nil?

  "'#{str.to_s.gsub(/\\/, '\&\&').gsub(/'/, "''")}'"
end

# Schools Cleaning

def clean_school_names(table_info, route)
  query = <<~SQL
    SELECT id, #{table_info[:raw_column]}
    FROM #{table_info[:raw_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  puts query.green
  school_names = route.query(query).to_a

  school_names.each do |item|
    puts JSON.pretty_generate(item).yellow
    puts table_info[:raw_column]
    clean_school_name = item[table_info[:raw_column]].dup
    clean_school_name.gsub!(/\bschl?\b/i, 'School')
    clean_school_name.gsub!(/\bdist\b/i, 'District')
    clean_school_name.gsub!(/\bel(em?)?\b/i, 'Elementary')
    clean_school_name.gsub!(/\bHS\b/i, 'High School')
    clean_school_name.gsub!(/\bJr\.?\b/i, 'Junior')
    clean_school_name.gsub!(/\bSr\.?\b/i, 'Senior')
    clean_school_name.gsub!(/\bCo\.?\b/i, 'County')
    clean_school_name.gsub!(/(?<![-'])\b([a-z]|Capt|Col|Dr|lt|Mt|Wm|Gr|St)(?=\s)(?!\.)/i) {|i| "#{i.capitalize}."}

    puts clean_school_name
    puts "#{item[table_info[:raw_column]]} >>> #{clean_school_name}".cyan if item[table_info[:raw_column]] != clean_school_name
    insert_query = <<~SQL
      UPDATE #{table_info[:raw_table]}
      SET #{table_info[:clean_column]} = '#{escape(clean_school_name)}'
      WHERE id = #{item['id']}
        AND #{table_info[:raw_column]}='#{escape(item[table_info[:raw_column]])}'
        AND #{table_info[:clean_column]} IS NULL;
    SQL

    puts insert_query
    route.query(insert_query)
  end
end
