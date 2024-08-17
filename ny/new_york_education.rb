# Creator:      Sergii Butrymenko
# Dataset Name: US Schools: New York education
# Task #:       109
# Scrape Task:  https://lokic.locallabs.com/scrape_tasks/656
# Data Set:     https://lokic.locallabs.com/data_sets/555
# Created:      April 2023

# ruby mlc.rb --tool="clean::ny::new_york_education" --mode='clean'
# ruby mlc.rb --tool="clean::ny::new_york_education" --mode='check_unmatched'

def execute(options = {})
  table_description = {
    clean: {
      raw_table: 'ny_general_info',
      raw_column: 'name',
      clean_column: 'name_clean',
    }
  }
  route = C::Mysql.on(DB01, 'us_schools_raw')
  mode = options['mode']&.to_sym
  table_info = table_description[mode]
  case mode
  when :clean
    clean_school_district_names(table_info, route)
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
    text: "*[CLEANING #109] US Schools: New York education* \n>#{type} #{message}",
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

# Schools & Districts Cleaning

def clean_school_district_names(table_info, route)
  query = <<~SQL
    SELECT id, #{table_info[:raw_column]}
    FROM #{table_info[:raw_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  puts query.green
  names = route.query(query).to_a

  names.each do |item|
    puts JSON.pretty_generate(item).yellow
    puts table_info[:raw_column]
    clean_name = item[table_info[:raw_column]].dup.split(/(?<!')\b(?![a-z]{2,})/)
                          .map{|item| %w(AECI BOCES CP CSD II III IV OCFS OMH).include?(item.upcase)  || item.match?(/^\d/) ? item.upcase : item.capitalize}
                          .map{|item| %w(and at for of the).include?(item.downcase) ? item.downcase : item}.join
    clean_name.gsub!(/\bschl?\b/i, 'School')
    clean_name.gsub!(/\bAdv\b/i, 'Advanced')
    clean_name.gsub!(/\Acad\b/i, 'Academy')
    clean_name.gsub!(/\b(CS|Chtr)\b/i, 'Charter School')
    clean_name.gsub!(/\bdist\b/i, 'District')
    clean_name.gsub!(/\bel(em?)?\b/i, 'Elementary')
    clean_name.gsub!(/\bMa(th)?\/Scie?\b/i, 'Math and Science')
    clean_name.gsub!(/\bHS\b/i, 'High School')
    clean_name.gsub!(/\bJr\.?\b/i, 'Junior')
    clean_name.gsub!(/\bNYC\b/i, 'New York City')
    clean_name.gsub!(/\bSD\b/i, 'School District')
    clean_name.gsub!(/\bSr\.?\b/i, 'Senior')
    clean_name.gsub!(/\bTechnol?\b/i, 'Technology')
    clean_name.gsub!(/\bCo\.?\b/i, 'County')
    clean_name.gsub!(/\b[imp]s\b/i) {|i| i.upcase}
    clean_name.gsub!(/(?<![-'])\b([a-z]|Capt|Col|Dr|lt|Mt|Wm|Gr|St)(?=\s)(?!\.)/i) {|i| "#{i.capitalize}."}
    clean_name.gsub!(/\s+\(THE\)/i, '')
    clean_name = clean_name.squeeze(' ')

    puts clean_name
    puts "#{item[table_info[:raw_column]]} >>> #{clean_name}".cyan if item[table_info[:raw_column]] != clean_name
    insert_query = <<~SQL
      UPDATE #{table_info[:raw_table]}
      SET #{table_info[:clean_column]} = '#{escape(clean_name)}'
      WHERE id = #{item['id']}
        AND #{table_info[:raw_column]}='#{escape(item[table_info[:raw_column]])}'
        AND #{table_info[:clean_column]} IS NULL;
    SQL

    puts insert_query
    route.query(insert_query)
  end
end
