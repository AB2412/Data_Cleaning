# COMMAND
# ruby mlc.rb --tool="clean::nv::nv_employee_salary" --job_type='new_clean' --years=2019,2020 --test
# PARAMETERS
# --years    -> required, may be one year, or few separated by commas
# --job_type -> required:
#               - new_clean   - clears where name_clean IS NULL OR job_title_clean IS NULL
#               - all_reclean - updates all rows in year(s)
# --test     -> optional, default = false, if true - won't update



MAIN_TABLE = 'nv_public_employee_salary'

def execute(options = {})
  parse_options(options)

  @exceptions = []
  @finished = []

  p @test = options.include?('test')
  @raw_data = []

  db01 = C::Mysql.on(DB01, 'usa_raw')

  years = options['years'] || raise('No years given!')


  case options['job_type']
  when 'new_clean'
    get_raw_data(db01, years)
  when 'all_reclean'
    get_raw_data(db01, years, new: false)
  else raise "No such job type: `#{options['job_type']}`!\n\n Only `new_clean` and `all_reclean` available."
  end

  @finished << "...#{@raw_data.size} ROWS FOUND TO CLEAN..."
  puts @finished.last.green

  clean_names
  clean_job_titles

  update_names_job_titles(db01)

rescue => err
  print format_err(err)
  send_slack_msg(err, error: true) unless err.message == 'No years given'
ensure
  db01.close if db01
  if @exceptions.any?
    @exceptions.uniq!
    send_slack_msg("*_Exceptions:_*\n\n#{@exceptions.join("\n\n\n")}", ping: true)
  else
    send_slack_msg("_Finished:_\n\n```#{@finished.join("\n")}```")
  end

end


def get_raw_data(db01, years, new: true)
  @finished << '...GETTING RAW DATA...'
  puts @finished.last.green

  prohibited_names = ['Not Provided',
                      '18 Under 18'].map{|n| "'#{n}'" }.join(',')

  query = <<~SQL
    SELECT
      id,
      full_name,
      first_name,
      middle_name,
      last_name,
      name_clean,
      job_title,
      job_title_clean
    FROM #{MAIN_TABLE}
    WHERE year IN (#{years.split(',').map{|y| "'#{y}'" }.join(',')})
      AND full_name NOT IN (#{prohibited_names})
      #{'AND (name_clean IS NULL OR job_title_clean IS NULL)' if new}
  SQL
  puts query

  @raw_data = db01.query(query, symbolize_keys: true).to_a
end

def clean_names
  @finished << '...CLEARING NAMES...'
  puts @finished.last.green

  @raw_data.map do |row|
    first_name = row[:first_name].gsub('.', '')
    middle_name = row[:middle_name]
    last_name = row[:last_name]
    full_name = "#{first_name} #{middle_name} #{last_name}"


    clean_name = if last_name.match?(/\s[IVX][IVX]+/)
                   "#{row[:first_name]} #{last_name}"
                 else
                   MiniLokiC::Formatize::Cleaner.person_clean(full_name, reverse = false)
                 end

    row[:name_clean] = escaped(clean_name)
  end
end

def clean_job_titles
  @finished << '...CLEARING JOB TITLES...'
  puts @finished.last.green

  @raw_data.map do |row|
    job_title = row[:job_title]

    clean_job_title = if job_title == job_title.upcase
                        lowercase_words = %w{a an the and but or for nor of in}
                        job_title.split('-').each_with_index.map { |x| x.split.map { |w| lowercase_words.include?(w.downcase) ? w.downcase : w.capitalize }.join(' ')}.join('-')
                      else
                        job_title
                      end

    row[:job_title_clean] = escaped(clean_job_title)
  end
end


def update_names_job_titles(db01)
  @finished << '...UPDATING NAMES AND JOB TITLES...'
  puts @finished.last.green

  @raw_data.each_slice(50).each do |batch|
    query = <<~SQL
      INSERT INTO #{MAIN_TABLE} 
        (id, name_clean, job_title_clean)
      VALUES
        #{batch.map{|row| "(#{row[:id]},'#{row[:name_clean]}','#{row[:job_title_clean]}')"}.join(",\n")}
      ON DUPLICATE KEY UPDATE 
        name_clean = VALUES(name_clean),
        job_title_clean = VALUES(job_title_clean);
    SQL

    db01.query(query) unless @test
  end
end

def escaped(str)
  res = str.valid_encoding? ? str : ascii_escape(str)
  res.gsub(/\\/, '').gsub(/\\*(\'|\"|\*|\(|\)|\]|\[|\/|\+|\\)/, '\\\\\1').strip
end



# Slack messages
def formatize_error(e)
  "```" + e.backtrace.map.with_index { |s, i| "#{i > 0 ? "#{i}: from " : "> "}#{s.gsub('`', "'")}#{"```" if i == 1}" }.reverse.join("\n") + ': ' + e.message.gsub(/\e\[[^\x40-\x7E]*[\x40-\x7E]/, '').split("\n").map { |s| "*#{s}*" }.join("\n> ") + " (_#{e.class}_)"
end

def send_slack_msg(message, ping: false, error: false)
  message = "#{"<@UCW1LMP2P>\n" if ping || error}*Nevada Employees Cleaner:*\n\n#{error ? formatize_error(message) : message}"
  Slack::Web::Client.new.chat_postMessage(
      channel: 'C02NDHDTJ8G',
      text: message,
      link_names: true,
      as_user: true
  )
end